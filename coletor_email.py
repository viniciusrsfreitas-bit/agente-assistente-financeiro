import email
import imaplib
import io
import json
import os
from email.header import decode_header
from email.message import Message
from pathlib import Path
from typing import List, Tuple

from dotenv import load_dotenv
from openai import OpenAI

from orquestrador_agente import processar_anexo_em_memoria, resultado_para_dict

PASTA_SAIDAS = Path("saidas")


def log(msg: str) -> None:
    print(f"[coletor_email] {msg}")


def decodificar_header(valor: str) -> str:
    if not valor:
        return ""
    partes = decode_header(valor)
    texto = []
    for conteudo, encoding in partes:
        if isinstance(conteudo, bytes):
            texto.append(conteudo.decode(encoding or "utf-8", errors="ignore"))
        else:
            texto.append(conteudo)
    return "".join(texto)


def extrair_corpo_texto(msg: Message) -> str:
    if msg.is_multipart():
        for parte in msg.walk():
            if parte.get_content_disposition() == "attachment":
                continue
            if parte.get_content_type() == "text/plain":
                payload = parte.get_payload(decode=True) or b""
                charset = parte.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="ignore")

        for parte in msg.walk():
            if parte.get_content_type() == "text/html":
                payload = parte.get_payload(decode=True) or b""
                charset = parte.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="ignore")
    else:
        payload = msg.get_payload(decode=True) or b""
        charset = msg.get_content_charset() or "utf-8"
        return payload.decode(charset, errors="ignore")

    return ""


def anexos_em_memoria(msg: Message) -> List[Tuple[str, io.BytesIO]]:
    anexos: List[Tuple[str, io.BytesIO]] = []
    for parte in msg.walk():
        if parte.get_content_disposition() != "attachment":
            continue
        nome = decodificar_header(parte.get_filename() or "anexo.bin")
        payload = parte.get_payload(decode=True)
        if payload:
            anexos.append((nome, io.BytesIO(payload)))
    return anexos


def perguntar_ia_triagem(cliente: OpenAI, modelo: str, assunto: str, corpo: str, anexos: List[str]) -> bool:
    pergunta = (
        "Este e-mail contém uma obrigação financeira (boleto, nota fiscal, fatura ou recibo) "
        "para processamento? Responda apenas SIM ou NÃO"
    )

    contexto = (
        f"Assunto: {assunto}\n"
        f"Corpo: {corpo[:8000]}\n"
        f"Nomes dos anexos: {', '.join(anexos) if anexos else '(sem anexos)'}\n"
        f"Pergunta: {pergunta}"
    )

    resposta = cliente.responses.create(
        model=modelo,
        input=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Você classifica e-mails financeiros. Responda somente SIM ou NÃO.",
                    }
                ],
            },
            {"role": "user", "content": [{"type": "input_text", "text": contexto}]},
        ],
        temperature=0,
        max_output_tokens=4,
    )

    texto = (resposta.output_text or "").strip().upper()
    return texto.startswith("SIM")


def conectar_imap() -> Tuple[imaplib.IMAP4_SSL, str]:
    host = os.getenv("IMAP_HOST", "")
    port = int(os.getenv("IMAP_PORT", "993"))
    usuario = os.getenv("EMAIL_USER", "")
    senha = os.getenv("EMAIL_PASSWORD", "")
    mailbox = os.getenv("IMAP_MAILBOX", "INBOX")

    if not all([host, usuario, senha]):
        raise ValueError("Defina IMAP_HOST, EMAIL_USER e EMAIL_PASSWORD no .env")

    conn = imaplib.IMAP4_SSL(host, port)
    conn.login(usuario, senha)
    conn.select(mailbox)
    return conn, mailbox


def salvar_resultados_json(resultados: List[dict]) -> None:
    PASTA_SAIDAS.mkdir(parents=True, exist_ok=True)
    stamp = Path(datetime_stamp()).name
    caminho = PASTA_SAIDAS / f"coleta_email_{stamp}.json"
    caminho.write_text(json.dumps({"resultados": resultados}, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Resultados salvos em {caminho}")


def datetime_stamp() -> str:
    from datetime import datetime

    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def processar_emails_nao_lidos() -> None:
    load_dotenv()
    cliente = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    modelo = os.getenv("OPENAI_MODEL", "gpt-5-mini")

    conn, mailbox = conectar_imap()
    log(f"Conectado ao IMAP em {mailbox}")

    status, data = conn.search(None, "UNSEEN")
    if status != "OK":
        log("Falha ao buscar e-mails não lidos.")
        conn.logout()
        return

    ids = data[0].split()
    log(f"E-mails não lidos encontrados: {len(ids)}")

    resultados: List[dict] = []

    for email_id in ids:
        status, payload = conn.fetch(email_id, "(RFC822)")
        if status != "OK" or not payload or not payload[0]:
            log(f"Falha ao carregar e-mail {email_id!r}")
            continue

        msg = email.message_from_bytes(payload[0][1])
        assunto = decodificar_header(msg.get("Subject", "(sem assunto)"))
        corpo = extrair_corpo_texto(msg)
        anexos = anexos_em_memoria(msg)
        nomes_anexos = [nome for nome, _ in anexos]

        log(f"Triagem IA do e-mail: {assunto}")
        try:
            processar = perguntar_ia_triagem(cliente, modelo, assunto, corpo, nomes_anexos)
        except Exception as exc:
            log(f"Erro na triagem IA: {exc}. Marcando como lido e ignorando.")
            processar = False

        if processar:
            log(f"IA => SIM. Processando {len(anexos)} anexos em memória (diskless).")
            for nome, buffer in anexos:
                try:
                    resultado = processar_anexo_em_memoria(nome, buffer)
                    resultados.append(resultado_para_dict(resultado))
                except Exception as exc:
                    log(f"Falha no anexo {nome}: {exc}")
                    resultados.append(
                        {
                            "arquivo_origem": nome,
                            "sucesso_extracao": False,
                            "erro": str(exc),
                        }
                    )
        else:
            log("IA => NÃO. E-mail ignorado.")

        conn.store(email_id, "+FLAGS", "\\Seen")

    conn.close()
    conn.logout()

    if resultados:
        salvar_resultados_json(resultados)

    log("Processamento finalizado.")


if __name__ == "__main__":
    processar_emails_nao_lidos()
