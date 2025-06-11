FROM astrocrpublic.azurecr.io/runtime:3.0-2

USER root

# Instalar dependências de sistema
RUN apt-get update && apt-get install -y \
    wget unzip curl gnupg ca-certificates \
    fonts-liberation libappindicator3-1 libasound2 libatk-bridge2.0-0 libatk1.0-0 \
    libgtk-3-0 libxss1 libnss3 libgconf-2-4 xdg-utils \
    gdal-bin libgdal-dev libproj-dev proj-data proj-bin \
    libgeos-dev libspatialindex-dev python3-dev build-essential \
    --no-install-recommends && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Instalar o Google Chrome diretamente do repositório
RUN curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/google-chrome-stable

# Instalar ChromeDriver diretamente compatível com a versão 137
RUN wget -O /tmp/chromedriver.zip "https://storage.googleapis.com/chrome-for-testing-public/137.0.7151.68/linux64/chromedriver-linux64.zip" && \
    unzip /tmp/chromedriver.zip -d /opt/chromedriver && \
    mv /opt/chromedriver/chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
    chmod +x /usr/bin/chromedriver && \
    rm -rf /tmp/chromedriver.zip /opt/chromedriver

# Instalar dependências Python
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Copiar scripts e dados
COPY scripts /usr/local/airflow/scripts
RUN chown -R astro:astro /usr/local/airflow/scripts
RUN chmod -R 755 /usr/local/airflow/scripts

COPY include/pts_disaster_export.xlsx /usr/local/airflow/include/pts_disaster_export.xlsx
COPY include/eswd_database.csv /usr/local/airflow/include/eswd_database.csv
COPY include/centroides /usr/local/airflow/include/centroides

USER astro
