Este projeto contém um scraper para recolher, classificar e processar notícias do Google News relacionadas com desastres naturais em Portugal. O sistema utiliza uma lista de palavras-chave e localidades (freguesias, municípios e distritos) para pesquisar notícias relevantes, extrair informações importantes e gerar datasets para análise posterior.

O pipeline é composto por várias etapas:
- **Scraping:** Pesquisa no Google News por combinações de palavras-chave e localidades, recolhendo títulos, links e datas das notícias.
- **Classificação:** Aplica heurísticas ou modelos de machine learning para identificar notícias relevantes sobre desastres.
- **Processamento:** Extrai informações detalhadas dos artigos, como tipo de desastre, número de vítimas, localizações e fonte da notícia.

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
   Ou para correr apenas uma etapa específica (exemplo: processar artigos relevantes):
   ```bash
   python3 main.py --etapa processar_relevantes
   ```

3. **Configuração**
   
   Certifique-se de que os ficheiros de configuração (`config/keywords.json` e `config/municipios_por_distrito.json`) estão presentes e corretamente preenchidos.

Os resultados intermédios e finais são guardados na pasta `data/`.
