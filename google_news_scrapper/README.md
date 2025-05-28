Este projeto contém um scraper para recolher, classificar e processar notícias do Google News relacionadas com desastres naturais em Portugal. O sistema utiliza uma lista de palavras-chave e localidades (freguesias, municípios e distritos) para pesquisar notícias relevantes, extrair informações importantes e gerar datasets para análise posterior.

O pipeline é composto por várias etapas:
- **Scraping:** Pesquisa no Google News por combinações de palavras-chave e localidades, recolhendo títulos, links e datas das notícias.
- **Classificação:** Aplica heurísticas ou modelos de machine learning para identificar notícias relevantes sobre desastres.
- **Processamento:** Extrai informações detalhadas dos artigos, como tipo de desastre, número de vítimas, localizações e fonte da notícia.
- **Exportação:** Envia os dados estruturados para uma base de dados PostgreSQL/Supabase.

## Componentes Principais

- **main.py**: Ponto de entrada principal para executar o pipeline completo
- **scraping/run_scraper.py**: Coleta notícias do Google News
- **processador/processar_relevantes.py**: Processa e estrutura as notícias coletadas
- **processador/filtrar_artigos_vitimas.py**: Filtra artigos com informações sobre vítimas
- **exportador_bd/export_to_supabase.py**: Exporta dados para o banco de dados
- **nlp/**: Modelos de processamento de linguagem natural para identificação de entidades

## Como correr o scraper

1. **Instalar dependências**
   
   Recomenda-se usar um ambiente virtual:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Executar o pipeline**
   
   Para correr todas as etapas do pipeline:
   ```bash
   python3 main.py --etapa all
   ```
   
   Para executar apenas uma etapa específica:
   ```bash
   python3 main.py --etapa run_scraper --dias 1
   ```

## Configuração

Os arquivos de configuração estão localizados no diretório `config/`:

- **keywords.json**: Palavras-chave para busca e filtro de notícias
- **municipios_por_distrito.json**: Lista de municípios e distritos de Portugal
- **freguesias_com_codigos.json**: Códigos oficiais das freguesias

## Modelos NLP

Os modelos de NLP treinados estão em `models/`:

- **modelo_classificacao.pkl**: Modelo para classificação de relevância
- **tfidf_vectorizer.pkl**: Vetorizador para processamento de texto
- **victims_nlp/**: Modelo spaCy para reconhecimento de entidades relacionadas a vítimas
   Para correr todas as etapas do pipeline:
   ```bash
   python3 main.py --etapa all
   ```

   Para coletar notícias de uma data específica:
   ```bash
   python3 scraping/run_scraper.py --date 2024-03-20
   ```

   Ou para correr apenas uma etapa específica (exemplo: processar artigos relevantes):
   ```bash
   python3 main.py --etapa processar_relevantes
   ```

3. **Configuração**
   
   Certifique-se de que os ficheiros de configuração (`config/keywords.json` e `config/municipios_por_distrito.json`) estão presentes e corretamente preenchidos.

## Estrutura de Dados

Os dados processados são organizados da seguinte forma:

- **data/raw/YYYY/MM/DD/**: Contém os ficheiros brutos organizados por ano/mês/dia
  - `artigos_filtrados_YYYYMMDD.csv`: Artigos filtrados por relevância
  - `artigos_google_municipios_pt_YYYYMMDD.csv`: Artigos processados com informações de localização
  - `artigos_[fonte]_YYYYMMDD.csv`: Artigos separados por fonte (jn, publico, etc.)

- **data/structured/**: Diretório para compatibilidade com versões anteriores
  - Contém os mesmos ficheiros que o diretório raw, mas sem a estrutura de pastas por data

Observação: Para manter a compatibilidade, os ficheiros são salvos tanto na estrutura de pastas por data quanto no diretório structured.

Os resultados intermédios e finais são guardados na pasta `data/`.
