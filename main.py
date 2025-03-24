import subprocess
import time
import os
import socket

# Função para verificar se a porta 8050 já está em uso (indicando que o Dashboard já está rodando)
def is_dashboard_running():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", 8050)) == 0

# Primeira execução: rodar os scrapers antes de iniciar o Dashboard
print("✅ Iniciando Web Scraping inicial...")

print("🔄 Executando scraper_anepc.py...")
subprocess.run(["python3", "scraper_anepc.py"], check=True)

print("🔄 Executando scraper_ipma.py...")
subprocess.run(["python3", "scraper_ipma.py"], check=True)

print("✅ Web Scraping inicial concluído!")

# Agora que o banco de dados foi criado, podemos iniciar o Dashboard
if not is_dashboard_running():
    print("📊 Iniciando Dashboard pela primeira vez...")
    dashboard_process = subprocess.Popen(["python3", "dashboard.py"])
    time.sleep(5)  # Espera para garantir que o Dashboard inicie corretamente
else:
    print("✅ Dashboard já está rodando. Apenas atualizando os dados.")

# Variável para controlar a última execução do scraper_ipma.py
ultima_execucao_ipma = time.time()

while True:
    print("✅ Iniciando Web Scraping...")

    print("🔄 Executando scraper_anepc.py...")
    subprocess.run(["python3", "scraper_anepc.py"], check=True)

    time.sleep(10)  # Pequena pausa

    # Verifica se já passaram 60 minutos desde a última execução do scraper_ipma.py
    tempo_atual = time.time()
    if tempo_atual - ultima_execucao_ipma >= 3600:  # 3600 segundos = 60 minutos
        print("🔄 Executando scraper_ipma.py...")
        subprocess.run(["python3", "scraper_ipma.py"], check=True)
        ultima_execucao_ipma = tempo_atual
    else:
        print(f"⏳ O scraper_ipma.py será executado novamente em {int(3600 - (tempo_atual - ultima_execucao_ipma))} segundos.")

    print("✅ Web Scraping concluído! Dashboard continuará rodando com os novos dados.")

    time.sleep(30)  # Aguarda 5 minutos antes do próximo ciclo
    
    #teste github
