# filtrar_cdxj_bulk.py (com progresso e debug)

import json
import csv
import re
import requests
import time
import urllib.parse

# ---------- CONFIGURAÇÕES INICIAIS ----------

CDXJ_URL = "https://arquivo.pt/datasets/cdxj/FAWP40.cdxj"
OUTPUT_CSV = "urls_filtrados.csv"
PADROES_DOMINIOS = [r"https?://(www\.)?publico\.pt", r"https?://(www\.)?dn\.pt"]
ANO_INICIO = 2020
ANO_FIM = 2024
PROGRESSO_CADA_N_LINHAS = 100000
DEBUG_MAX_POR_BLOCO = 100  # número máximo de prints de debug por bloco de 1M
PRINT_PRIMEIRAS_N_URLS = 50

# ---------- FUNÇÃO AUXILIAR PARA CONSTRUIR LINKS ----------
def construir_links(timestamp, url):
    url_replay = f"https://arquivo.pt/noFrame/replay/{timestamp}/{url}"
    url_replay_id = f"https://arquivo.pt/noFrame/replay/{timestamp}id_/{url}"
    url_encoded = urllib.parse.quote(f"{url}//{timestamp}", safe='')
    url_text = f"https://arquivo.pt/textextracted?m={url_encoded}"
    return url_replay, url_replay_id, url_text

# ---------- PROCESSAMENTO DIRETO DO STREAM ----------
def processar_stream():
    total = 0
    validos = 0
    debug_count = 0
    debug_candidatos = []
    urls_mostradas = 0
    inicio = time.time()

    print(f"\n⬇️ A descarregar e filtrar diretamente de: {CDXJ_URL}\n")
    try:
        r = requests.get(CDXJ_URL, stream=True)
        r.raise_for_status()
    except Exception as e:
        print(f"❌ Erro ao tentar aceder ao CDXJ: {e}")
        return

    try:
        f = open(OUTPUT_CSV, "w", newline="", encoding="utf-8")
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp", "url", "mime", "status", "filename", "offset", "digest", "collection",
            "link_replay", "link_replay_id", "link_text"
        ])
        writer.writeheader()
    except Exception as e:
        print(f"❌ Erro ao abrir ficheiro CSV: {e}")
        return

    for linha_bytes in r.iter_lines():
        total += 1

        if total % 10000 == 0:
            print(f"⌛ {total:,} linhas lidas...")

        try:
            linha = linha_bytes.decode("utf-8")

            if any(re.search(pat, linha) for pat in PADROES_DOMINIOS):
                print(f"🧪 Match bruto na linha #{total}: {linha}")

            partes = linha.strip().split(" ", 1)
            if len(partes) != 2:
                continue

            timestamp = partes[0]
            if not timestamp.isdigit():
                continue
            ano = int(timestamp[:4])
            if not (ANO_INICIO <= ano <= ANO_FIM):
                continue

            try:
                dados = json.loads(partes[1])
            except json.JSONDecodeError as e:
                print(f"⚠️ Erro ao decodificar JSON na linha #{total}: {e}")
                continue

            url = dados.get("url") or dados.get("original") or ""
            status = str(dados.get("status"))
            mime = str(dados.get("mime") or "").lower()

            if urls_mostradas < PRINT_PRIMEIRAS_N_URLS:
                print(f"🔗 Linha #{total}: {url} | status={status}, mime={mime}")
                urls_mostradas += 1

            if any(re.search(pat, url) for pat in PADROES_DOMINIOS):
                if debug_count < DEBUG_MAX_POR_BLOCO:
                    debug_candidatos.append(f"[DEBUG] URL candidato: {url} | status={status}, mime={mime}")
                    debug_count += 1

                if status.startswith("2") and "html" in mime:
                    replay, replay_id, text_link = construir_links(timestamp, url)
                    writer.writerow({
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
                    })
                    validos += 1
                    print(f"✅ Linha #{total} gravada no CSV: {url}")

        except Exception as e:
            print(f"⚠️ Erro ao processar linha #{total}: {e}")
            continue

        if total % PROGRESSO_CADA_N_LINHAS == 0:
            tempo_passado = time.time() - inicio
            print(f"\n🧶 {total:,} linhas lidas... {validos:,} válidas até agora. ({tempo_passado:.1f}s)")
            if debug_candidatos:
                print("\n🪵 Debug de candidatos recentes:")
                for linha_debug in debug_candidatos:
                    print(linha_debug)
                debug_candidatos = []
                debug_count = 0

    tempo_total = time.time() - inicio
    print(f"\n🔍 Total linhas lidas: {total}")
    print(f"✅ URLs válidos encontrados: {validos}")
    print(f"⏱️ Tempo total: {tempo_total:.1f} segundos")

    f.close()
    if validos > 0:
        print(f"📦 Exportado para {OUTPUT_CSV}")
    else:
        print("⚠️ Nenhum URL válido encontrado. Nenhum ficheiro exportado.")

if __name__ == "__main__":
    processar_stream()
