#!/bin/bash

# Make sure environment file exists
if [ ! -f .env ]; then
  echo "Creating .env file from template..."
  cp .env.example .env
fi

# Install python dependencies
pip install -r requirements.txt

# Download spaCy models
python -m spacy download pt_core_news_lg
python -m spacy download en_core_web_lg

# Download NLTK data
python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords'); nltk.download('wordnet')"

# Create necessary directories if they don't exist
mkdir -p data/raw data/structured

# Verify Chrome installation
echo "Verifying Chrome and ChromeDriver installation..."
python tests/verify_chrome_installation.py

echo "Setup completed successfully."
