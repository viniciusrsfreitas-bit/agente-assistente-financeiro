import os
import shutil
from typing import Dict

import pdfplumber

PASTA_ENTRADAS = "entradas"
PASTA_PROCESSADOS = "processados"
PASTA_GRAFICOS = "graficos"


def garantir_pastas() -> None:
    """Cria as pastas necessárias caso ainda não existam."""
    for pasta in (PASTA_ENTRADAS, PASTA_PROCESSADOS, PASTA_GRAFICOS):
        if not os.path.exists(pasta):
            print(f"Pasta '{pasta}' não existe. Criando...")
            os.makedirs(pasta, exist_ok=True)
        else:
            print(f"Pasta '{pasta}' já existe.")


def extrair_texto_pdfs() -> Dict[str, str]:
    """
    Lê todos os PDFs da pasta de entradas, extrai o texto de todas as páginas,
    armazena temporariamente em memória e move os arquivos para processados.

    Returns:
        Dict[str, str]: dicionário no formato {nome_arquivo: texto_extraido}
    """
    textos_extraidos: Dict[str, str] = {}

    arquivos_pdf = [
        nome_arquivo
        for nome_arquivo in os.listdir(PASTA_ENTRADAS)
        if nome_arquivo.lower().endswith(".pdf")
    ]

    if not arquivos_pdf:
        print("Nenhum arquivo PDF encontrado na pasta 'entradas'.")
        return textos_extraidos

    for nome_arquivo in arquivos_pdf:
        caminho_entrada = os.path.join(PASTA_ENTRADAS, nome_arquivo)
        caminho_processado = os.path.join(PASTA_PROCESSADOS, nome_arquivo)

        print(f"Lendo arquivo '{nome_arquivo}'...")

        texto_paginas = []
        with pdfplumber.open(caminho_entrada) as pdf:
            for indice_pagina, pagina in enumerate(pdf.pages, start=1):
                texto = pagina.extract_text() or ""
                texto_paginas.append(texto)
                print(
                    f"Página {indice_pagina} de '{nome_arquivo}' lida com sucesso."
                )

        texto_completo = "\n".join(texto_paginas).strip()
        textos_extraidos[nome_arquivo] = texto_completo
        print(f"Texto extraído com sucesso do arquivo '{nome_arquivo}'.")

        print(
            f"Movendo arquivo '{nome_arquivo}' de '{PASTA_ENTRADAS}' para '{PASTA_PROCESSADOS}'..."
        )
        shutil.move(caminho_entrada, caminho_processado)
        print(f"Arquivo '{nome_arquivo}' movido com sucesso.")

    return textos_extraidos


def main() -> None:
    print("Iniciando processamento de PDFs...")
    garantir_pastas()
    textos = extrair_texto_pdfs()
    print(f"Processamento finalizado. Total de arquivos processados: {len(textos)}")


if __name__ == "__main__":
    main()
