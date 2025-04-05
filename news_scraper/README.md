# Scraper de Notícias para Eventos de Desastres

Este projeto é um scraper de notícias projetado para coletar e processar artigos relacionados a desastres de várias fontes. Ele utiliza feeds RSS do Google News e outras ferramentas para extrair, filtrar e transformar dados em um formato estruturado para análise posterior.

## Funcionalidades

- **Resolução Dinâmica de URLs**: Resolve URLs reais de links do Google News usando Selenium.
- **Extração de Conteúdo**: Extrai o texto dos artigos usando BeautifulSoup e Selenium.
- **Detecção de Desastres**: Identifica tipos de desastres, número de vítimas e locais afetados.
- **Armazenamento de Dados**: Salva os dados processados em arquivos CSV para fácil acesso e análise.
- **Mapeamento de Localização**: Mapeia artigos para municípios, distritos e freguesias usando configurações JSON predefinidas.

## Estrutura do Projeto

- **`scrapers/`**: Contém a lógica de scraping, incluindo os scrapers do Google News e Arquivo.pt.
- **`processors/`**: Inclui scripts para filtrar e transformar dados brutos.
- **`utils/`**: Funções utilitárias para normalização, localização e manipulação de dados.
- **`config/`**: Arquivos JSON para palavras-chave, municípios e códigos de freguesias.
- **`data/`**: Diretório para armazenar os arquivos CSV de saída.

## Como Executar

1. **Instalar Dependências**:
   Certifique-se de ter o Python instalado. Instale as bibliotecas necessárias usando:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configurar o ChromeDriver**:
   Baixe e coloque o binário do ChromeDriver no PATH do sistema ou atualize o `driver_path` no código.

3. **Executar o Scraper**:
   Execute o script principal para iniciar o pipeline de scraping:
   ```bash
   python main.py
   ```

4. **Saída**:
   Os artigos processados serão salvos no diretório `data/`.

## Configuração

- **Palavras-chave**: Atualize `config/keywords.json` para adicionar ou modificar palavras-chave para detecção de desastres.
- **Municípios**: Atualize `config/municipios_por_distrito.json` para mapeamento de localizações.
- **Códigos de Freguesias**: Atualize `config/freguesias_com_codigos.json` para mapeamento de códigos de freguesias.

## Requisitos

- Python 3.7+
- Selenium
- BeautifulSoup
- Pandas
- ChromeDriver
