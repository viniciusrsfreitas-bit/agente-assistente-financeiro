import base64
import io
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import fitz  # PyMuPDF
from cryptography.fernet import Fernet
from openai import OpenAI
from PIL import Image

PASTA_BACKUP_CRIPTO = Path("backups_criptografados")


@dataclass
class ResultadoProcessamento:
    arquivo_origem: str
    tipo_documento: str
    cnpj_favorecido: Optional[str]
    nome_favorecido: Optional[str]
    cnpj_pagador: Optional[str]
    nome_pagador: Optional[str]
    vencimento: Optional[str]
    valor: Optional[float]
    numero_identificacao: Optional[str]
    linha_digitavel: Optional[str]
    boleto_prova_real_ok: Optional[bool]
    sucesso_extracao: bool


def log(msg: str) -> None:
    print(f"[orquestrador] {msg}")


def _bytes_to_images(arquivo_nome: str, conteudo: bytes) -> List[Image.Image]:
    sufixo = Path(arquivo_nome).suffix.lower()

    if sufixo == ".pdf":
        imagens: List[Image.Image] = []
        pdf = fitz.open(stream=conteudo, filetype="pdf")
        for pagina in pdf:
            pix = pagina.get_pixmap(dpi=200)
            img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
            imagens.append(img)
        pdf.close()
        return imagens

    if sufixo in {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}:
        return [Image.open(io.BytesIO(conteudo)).convert("RGB")]

    raise ValueError(f"Formato não suportado para processamento em memória: {sufixo}")


def _image_to_data_url(img: Image.Image) -> str:
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    b64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
    return f"data:image/png;base64,{b64}"


def _extrair_com_ia(imagens: List[Image.Image], modelo: str) -> Dict[str, Optional[str]]:
    cliente = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    instrucoes = (
        "Você é um extrator financeiro multimodal. "
        "Classifique o documento como BOLETO ou NF e extraia campos por ancoragem visual. "
        "Retorne SOMENTE JSON válido com as chaves: "
        "tipo_documento, cnpj_favorecido, nome_favorecido, cnpj_pagador, nome_pagador, "
        "vencimento, valor, numero_identificacao, linha_digitavel."
    )

    conteudo_usuario = [
        {
            "type": "input_text",
            "text": "Extraia os campos financeiros. Use null quando não encontrar.",
        }
    ]

    for img in imagens:
        conteudo_usuario.append(
            {
                "type": "input_image",
                "image_url": _image_to_data_url(img),
            }
        )

    resposta = cliente.responses.create(
        model=modelo,
        input=[
            {"role": "system", "content": [{"type": "input_text", "text": instrucoes}]},
            {"role": "user", "content": conteudo_usuario},
        ],
        temperature=0,
    )

    texto = (resposta.output_text or "").strip()
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        bloco = re.search(r"\{.*\}", texto, re.DOTALL)
        if not bloco:
            raise ValueError("IA não retornou JSON válido")
        return json.loads(bloco.group(0))


def _normalizar_cnpj(cnpj: Optional[str]) -> Optional[str]:
    if not cnpj:
        return None
    digitos = re.sub(r"\D", "", cnpj)
    return digitos if len(digitos) == 14 else None


def _normalizar_valor(valor: Optional[str]) -> Optional[float]:
    if valor is None:
        return None
    if isinstance(valor, (int, float)):
        return float(valor)

    txt = str(valor).replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(txt)
    except ValueError:
        return None


def _normalizar_data_iso(data_raw: Optional[str]) -> Optional[str]:
    if not data_raw:
        return None
    texto = str(data_raw).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(texto, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _normalizar_linha_digitavel(valor: Optional[str]) -> Optional[str]:
    if not valor:
        return None
    digitos = re.sub(r"\D", "", str(valor))
    return digitos if len(digitos) in {47, 48} else None


def _linha47_para_barcode(linha: str) -> str:
    c1 = linha[0:10]
    c2 = linha[10:21]
    c3 = linha[21:32]
    c4 = linha[32]
    c5 = linha[33:47]
    return c1[0:4] + c4 + c5 + c1[4:9] + c2[0:10] + c3[0:10]


def _datas_possiveis_por_fator(fator: int) -> List[date]:
    base_antiga = date(1997, 10, 7)
    base_nova = date(2022, 5, 29)
    datas = [base_antiga + timedelta(days=fator)]
    if fator >= 1000:
        datas.append(base_nova + timedelta(days=fator))
    return datas


def validar_boleto_prova_real(linha_digitavel: Optional[str], valor: Optional[float], vencimento_iso: Optional[str]) -> Optional[bool]:
    linha = _normalizar_linha_digitavel(linha_digitavel)
    if not linha or len(linha) != 47:
        return None

    barcode = _linha47_para_barcode(linha)
    fator = int(barcode[5:9])
    valor_barra = int(barcode[9:19]) / 100.0

    valor_ok = valor is not None and abs(valor - valor_barra) < 0.009

    data_ok = False
    if vencimento_iso:
        try:
            venc = datetime.strptime(vencimento_iso, "%Y-%m-%d").date()
            data_ok = venc in _datas_possiveis_por_fator(fator)
        except ValueError:
            data_ok = False

    return valor_ok and data_ok


def _salvar_backup_criptografado(arquivo_nome: str, conteudo: bytes) -> Optional[Path]:
    chave = os.getenv("BACKUP_ENCRYPTION_KEY", "")
    if not chave:
        log("BACKUP_ENCRYPTION_KEY ausente. Backup criptografado opcional não realizado.")
        return None

    PASTA_BACKUP_CRIPTO.mkdir(parents=True, exist_ok=True)
    fernet = Fernet(chave.encode("utf-8"))
    criptografado = fernet.encrypt(conteudo)

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    destino = PASTA_BACKUP_CRIPTO / f"{Path(arquivo_nome).stem}_{stamp}.bin"
    destino.write_bytes(criptografado)
    return destino


def processar_anexo_em_memoria(arquivo_nome: str, buffer: io.BytesIO) -> ResultadoProcessamento:
    conteudo = buffer.getvalue()
    imagens = _bytes_to_images(arquivo_nome, conteudo)
    dados = _extrair_com_ia(imagens, modelo=os.getenv("OPENAI_MODEL", "gpt-5-mini"))

    tipo = str(dados.get("tipo_documento") or "NF").upper()
    cnpj_fav = _normalizar_cnpj(dados.get("cnpj_favorecido"))
    cnpj_pag = _normalizar_cnpj(dados.get("cnpj_pagador"))
    valor = _normalizar_valor(dados.get("valor"))
    venc = _normalizar_data_iso(dados.get("vencimento"))
    linha = _normalizar_linha_digitavel(dados.get("linha_digitavel"))

    prova_real = None
    if tipo == "BOLETO":
        prova_real = validar_boleto_prova_real(linha, valor, venc)

    sucesso = True
    if tipo == "BOLETO" and prova_real is False:
        sucesso = False

    resultado = ResultadoProcessamento(
        arquivo_origem=arquivo_nome,
        tipo_documento=tipo,
        cnpj_favorecido=cnpj_fav,
        nome_favorecido=dados.get("nome_favorecido"),
        cnpj_pagador=cnpj_pag,
        nome_pagador=dados.get("nome_pagador"),
        vencimento=venc,
        valor=valor,
        numero_identificacao=dados.get("numero_identificacao"),
        linha_digitavel=linha,
        boleto_prova_real_ok=prova_real,
        sucesso_extracao=sucesso,
    )

    if sucesso:
        backup = _salvar_backup_criptografado(arquivo_nome, conteudo)
        if backup:
            log(f"Backup criptografado salvo: {backup}")

    return resultado


def resultado_para_dict(resultado: ResultadoProcessamento) -> Dict[str, object]:
    return asdict(resultado)
