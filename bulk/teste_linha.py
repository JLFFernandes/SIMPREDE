import re
import json
import urllib.parse

# Linha de exemplo vinda de https://arquivo.pt
linha_teste = '20200101180542 {"url": "https://www.publico.pt/", "mime": "text/html", "status": "200", "digest": "Y6LOLZTYUDL24X2ROLQU7GQ2WH4UWVXR", "length": "109185", "offset": "18099522", "filename": "WEB-20200101180502707-p84.arquivo.pt.warc.gz", "collection": "FAWP40"}'

# Configurações simuladas (iguais ao script original)
PADROES_DOMINIOS = [r"https?://(www\.)?publico\.pt", r"https?://(www\.)?dn\.pt"]
ANO_INICIO = 2020
ANO_FIM = 2024

# Função auxiliar para construir links
def construir_links(timestamp, url):
    url_replay = f"https://arquivo.pt/noFrame/replay/{timestamp}/{url}"
    url_replay_id = f"https://arquivo.pt/noFrame/replay/{timestamp}id_/{url}"
    url_encoded = urllib.parse.quote(f"{url}//{timestamp}", safe='')
    url_text = f"https://arquivo.pt/textextracted?m={url_encoded}"
    return url_replay, url_replay_id, url_text

# 1. Separar timestamp e JSON
partes = linha_teste.strip().split(" ", 1)
timestamp = partes[0]
dados_json = json.loads(partes[1])

# 2. Validar ano
ano = int(timestamp[:4])
if not (ANO_INICIO <= ano <= ANO_FIM):
    print(f"❌ Ano fora do intervalo: {ano}")
else:
    print(f"✅ Ano dentro do intervalo: {ano}")

# 3. Extrair dados
url = dados_json.get("url", "")
status = str(dados_json.get("status"))
mime = str(dados_json.get("mime")).lower()

# 4. Verificar se corresponde aos domínios pretendidos
if any(re.search(pat, url) for pat in PADROES_DOMINIOS):
    print(f"✅ Domínio reconhecido: {url}")
else:
    print(f"❌ Domínio NÃO reconhecido: {url}")

# 5. Verificar status e mime
if status.startswith("2") and "html" in mime:
    print(f"✅ Status e MIME válidos: {status}, {mime}")
    replay, replay_id, text_link = construir_links(timestamp, url)
    print("\n📦 Dados prontos para exportar:")
    print(f" - Timestamp: {timestamp}")
    print(f" - URL: {url}")
    print(f" - Replay: {replay}")
    print(f" - Texto extraído: {text_link}")
else:
    print(f"❌ Ignorado por status ou mime: {status}, {mime}")
