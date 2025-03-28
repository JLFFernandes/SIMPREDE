import pandas as pd
import os

file_path = '/home/odiurdigital/Universidade/projeto/simprede/scrapers/news_scraper/data/artigos_filtrados.csv'

if not os.path.exists(file_path) or os.stat(file_path).st_size == 0:
    print(f"⚠️ The file '{file_path}' is missing or empty.")
    df = pd.DataFrame()  # Create an empty DataFrame
else:
    # Load the CSV file
    df = pd.read_csv(file_path)

print(df)