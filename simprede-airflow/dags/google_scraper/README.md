# Google News Scraper

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

## Execução em Docker (Recomendado)

Este componente foi configurado para ser executado dentro de um container Docker, especialmente quando integrado com o Airflow.

1. **Verificar a instalação do Docker**
   ```bash
   docker --version
   ```

2. **Construir a imagem Docker**
   ```bash
   docker build -t google-news-scraper .
   ```

3. **Executar o scraper no Docker**
   ```bash
   docker run -v $(pwd)/data:/app/data google-news-scraper python scraping/run_scraper.py --dias 1
   ```

4. **Verificar o estado do scraper**
   ```bash
   docker run google-news-scraper python health_check.py
   ```

## Execução Local (Desenvolvimento)

1. **Instalar dependências**
   
   Recomenda-se usar um ambiente virtual:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   python -m spacy download pt_core_news_lg
   python -m spacy download en_core_web_lg
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
- **eventos_climaticos.json**: Tipos de eventos climáticos extremos
- **municipios_por_distrito.json**: Lista de municípios e distritos de Portugal
- **freguesias_com_codigos.json**: Códigos oficiais das freguesias
- **user_agents.txt**: Lista de user agents para o scraper

## Modelos NLP

Os modelos de NLP treinados estão em `models/`:

- **modelo_classificacao.pkl**: Modelo para classificação de relevância
- **tfidf_vectorizer.pkl**: Vetorizador para processamento de texto
- **victims_nlp/**: Modelo spaCy para reconhecimento de entidades relacionadas a vítimas

## Estrutura de Dados

Os dados processados são organizados da seguinte forma:

- **data/raw/YYYY/MM/DD/**: Contém os ficheiros brutos organizados por ano/mês/dia
  - `artigos_filtrados_YYYYMMDD.csv`: Artigos filtrados por relevância
  - `artigos_google_municipios_pt_YYYYMMDD.csv`: Artigos processados com informações de localização
  - `artigos_[fonte]_YYYYMMDD.csv`: Artigos separados por fonte (jn, publico, etc.)

- **data/structured/**: Diretório para compatibilidade com versões anteriores
  - Contém os mesmos ficheiros que o diretório raw, mas sem a estrutura de pastas por data

## Integração com Airflow

Este scraper está configurado para ser executado como parte de um pipeline Airflow. O DAG correspondente está em `airflow/dags/google_scraper_dag.py`.

### Utilizando os Scripts Específicos para Airflow

Foram criadas versões específicas dos scripts para serem executadas pelo Airflow:

- `scraping/run_scraper_airflow.py`: Versão do scraper otimizada para Airflow
- `processador/processar_relevantes_airflow.py`: Processador de relevância para Airflow
- `processador/filtrar_artigos_vitimas_airflow.py`: Filtro de artigos para Airflow
- `exportador_bd/export_to_supabase_airflow.py`: Exportador para Airflow

Estas versões são projetadas para funcionar de forma não interativa e com melhor integração com o Airflow.

### Execução do DAG no Airflow

Para executar o pipeline através do Airflow:

1. **Via Interface Web**:
   - Acesse o dashboard do Airflow (geralmente em http://localhost:8080)
   - Localize o DAG `google_scraper_pipeline`
   - Clique em "Trigger DAG" e adicione parâmetros de configuração opcionais:
     ```json
     {
       "dias": 3,
       "date": "2025-06-01"
     }
     ```

2. **Via Linha de Comando**:
   ```bash
   airflow dags trigger google_scraper_pipeline --conf '{"dias": 3, "date": "2025-06-01"}'
   ```

3. **Via Script de Referência**:
   ```bash
   python main_airflow.py --info
   ```

### Parâmetros do DAG

- `dias`: Número de dias anteriores a considerar (default: 1)
- `date`: Data específica para processar no formato YYYY-MM-DD (opcional)

O pipeline está configurado para ser executado diariamente de acordo com a programação definida no DAG.

Para testar a integração, use os comandos do Makefile:
```bash
make scraper-run     # Executa apenas o scraper
make scraper-process # Executa apenas o processamento
make scraper-shell   # Abre um shell no container do scraper
make scraper-health  # Verifica o estado do scraper
```

## Troubleshooting

Se encontrar problemas:

1. Verifique os logs do container Docker: `docker logs simprede-google-news-scraper-1`
2. Execute o script de verificação de saúde: `python health_check.py`
3. Certifique-se de que o Chrome está instalado e acessível
4. Verifique se todos os modelos spaCy necessários estão baixados
5. Tente executar os comandos manualmente dentro do container: `docker exec -it simprede-google-news-scraper-1 bash`
