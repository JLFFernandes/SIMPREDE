# filtrar_cdxj_bulk_multiprocessing.py

import json
import csv
import re
import requests
import time
import urllib.parse
from multiprocessing import Pool, cpu_count

# ---------- CONFIGURAÇÕES INICIAIS ----------
CDXJ_URL = "https://arquivo.pt/datasets/cdxj/FAWP40.cdxj"
OUTPUT_CSV = "urls_filtrados.csv"
PADROES_DOMINIOS = [r"https?://(www\.)?publico\.pt", r"https?://(www\.)?dn\.pt"]
ANO_INICIO = 2020
ANO_FIM = 2024
BATCH_SIZE = 10000

# ---------- FUNÇÃO AUXILIAR PARA CONSTRUIR LINKS ----------
def construir_links(timestamp, url):
    url_replay = f"https://arquivo.pt/noFrame/replay/{timestamp}/{url}"
    url_replay_id = f"https://arquivo.pt/noFrame/replay/{timestamp}id_/{url}"
    url_encoded = urllib.parse.quote(f"{url}//{timestamp}", safe='')
    url_text = f"https://arquivo.pt/textextracted?m={url_encoded}"
    return url_replay, url_replay_id, url_text

# ---------- FUNÇÃO DE PROCESSAMENTO DE CADA LINHA ----------
def processar_linha(linha):
    try:
        partes = linha.strip().split(" ", 1)
        if len(partes) != 2:
            return None

        timestamp = partes[0]
        if not timestamp.isdigit():
            return None

        ano = int(timestamp[:4])
        if not (ANO_INICIO <= ano <= ANO_FIM):
            return None

        dados = json.loads(partes[1])
        url = dados.get("url") or dados.get("original") or ""
        status = str(dados.get("status"))
        mime = str(dados.get("mime") or "").lower()

        if any(re.search(pat, url) for pat in PADROES_DOMINIOS) and status.startswith("2") and "html" in mime:
            replay, replay_id, text_link = construir_links(timestamp, url)
            return {
                "timestamp": timestamp,
                "url": url,
                "mime": mime,
                "status": status,
                "filename": dados.get("filename", ""),
                "offset": dados.get("offset", ""),
                "digest": dados.get("digest", ""),
                "collection": dados.get("collection", ""),
                "link_replay": replay,
                "link_replay_id": replay_id,
                "link_text": text_link
            }

    except Exception:
        return None

# ---------- FUNÇÃO PRINCIPAL COM MULTIPROCESSING ----------
def processar_stream_multiprocessing():
    inicio = time.time()

    print(f"\n⬇️ Iniciando descarga de: {CDXJ_URL}\n")

    try:
        r = requests.get(CDXJ_URL, stream=True)
        r.raise_for_status()
    except Exception as e:
        print(f"❌ Erro ao aceder ao CDXJ: {e}")
        return

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp", "url", "mime", "status", "filename", "offset", "digest", "collection",
            "link_replay", "link_replay_id", "link_text"
        ])
        writer.writeheader()

        pool = Pool(cpu_count())

        linhas_batch = []
        total = 0
        validos = 0

        for linha_bytes in r.iter_lines():
            linhas_batch.append(linha_bytes.decode("utf-8"))

            if len(linhas_batch) >= BATCH_SIZE:
                resultados = pool.map(processar_linha, linhas_batch)
                for res in resultados:
                    if res:
                        writer.writerow(res)
                        validos += 1

                total += len(linhas_batch)
                print(f"⌛ Processadas {total:,} linhas... {validos:,} URLs válidos.")

                linhas_batch = []

        if linhas_batch:
            resultados = pool.map(processar_linha, linhas_batch)
            for res in resultados:
                if res:
                    writer.writerow(res)
                    validos += 1

    pool.close()
    pool.join()

    tempo_total = time.time() - inicio
    print(f"\n🔍 Total de linhas processadas: {total}")
    print(f"✅ URLs válidos encontrados: {validos}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s")
    print(f"📦 Dados exportados para {OUTPUT_CSV}")

if __name__ == "__main__":
    processar_stream_multiprocessing()