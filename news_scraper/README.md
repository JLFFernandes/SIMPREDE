# 📚 Projeto de Extração de Artigos sobre Desastres Naturais

Este projeto tem como objetivo **recolher, filtrar e enriquecer artigos de notícias relacionadas com desastres naturais** (como cheias, inundações, tempestades) publicados em Portugal.

Usa como fonte principal o **Arquivo.pt**, extraindo notícias do **Diário de Notícias** e do **Público** entre os anos de **2020 e 2024**. O objetivo final é gerar uma base de dados pronta para uso analítico: `disaster_db_ready.csv`.

---

## 🧩 Estrutura Geral do Projeto

```bash
news_scraper/
├── config/                # Configurações de keywords e municípios
├── data/                  # Dados extraídos e processados
├── processors/            # Scripts de limpeza e transformação
├── scrapers/              # Scrapers (ex: Arquivo.pt)
├── utils/                 # Funções auxiliares
├── main.py                # Ponto de entrada principal
├── requirements.txt       # Dependências do projeto
└── README.md              # Este ficheiro
```

---

## 🔍 Etapas do Processo

### 1. **Coleta de Dados (Arquivo.pt)**

- O ficheiro `scrapers/arquivo_pt.py`:
  - Acede à API do Arquivo.pt.
  - Pesquisa notícias com combinações `keyword + município`.
  - Extrai título, texto principal, data, e metadados.

### 2. **Filtragem e Classificação**

- O ficheiro `processors/filtra_desastres.py`:
  - Aplica filtros semânticos para garantir que os artigos são relevantes.
  - Identifica tipo e subtipo de desastre.
  - Extrai número de vítimas, hora do evento e município mencionado.
  - Guarda o resultado em `artigos_filtrados.csv`.

### 3. **Transformação Final**

- O ficheiro `processors/gera_db_ready.py`:
  - Limpa, normaliza e estrutura os dados para análise.
  - Produz o ficheiro `disaster_db_ready.csv` com os seguintes campos principais:

```csv
ID, type, subtype, date, year, month, day, hour,
georef, district, municipali, parish, DICOFREG,
source, sourcedate, sourcetype, page,
fatalities, injured, evacuated, displaced, missing,
DataScraping
```

---

## ⚙️ Requisitos

Instalar dependências com:

```bash
pip install -r requirements.txt
```

---

## ▶️ Como Executar

1. **Raspar artigos do Arquivo.pt**:

```bash
python scrapers/arquivo_pt.py
```

2. **Filtrar artigos relevantes sobre desastres**:

```bash
python processors/filtra_desastres.py
```

3. **Gerar dataset final para análise**:

```bash
python processors/gera_db_ready.py
```

---

## 📦 Output Esperado

- `artigos_filtrados.csv`: artigos classificados como relevantes.
- `disaster_db_ready.csv`: dataset limpo e estruturado, pronto para análise.

---

## 🧠 Exemplos de Keywords (`config/keywords.json`)

```json
{
  "weather_terms": {
    "portuguese": [
      "cheias", "inundação", "deslizamento", "tempestade", ...
    ]
  }
}
```

---

## 🗺️ Georreferenciação (`config/municipios.json`)

Contém todos os municípios portugueses para localizar geograficamente os eventos extraídos.

