import spacy
import pandas as pd
import os
import re
import sys
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time

MODEL_PATH = os.environ.get("VICTIM_NER_MODEL_PATH", "../models/victims_nlp")
MODEL_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), MODEL_PATH))
INPUT_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/structured/artigos_google_municipios_pt.csv"))
OUTPUT_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/raw/ner_victims_list.csv"))

PT_NUMBER_WORDS = {
    "um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "quatro": 4, "cinco": 5, "seis": 6, "sete": 7,
    "oito": 8, "nove": 9, "dez": 10, "onze": 11, "doze": 12, "treze": 13, "catorze": 14, "quatorze": 14,
    "quinze": 15, "dezasseis": 16, "dezesseis": 16, "dezessete": 17, "dezanove": 19, "dezoito": 18, "dezanove": 19,
    "vinte": 20, "trinta": 30, "quarenta": 40, "cinquenta": 50, "sessenta": 60, "setenta": 70, "oitenta": 80, "noventa": 90,
    "cem": 100, "cento": 100, "duzentos": 200, "trezentos": 300, "quatrocentos": 400, "quinhentos": 500,
    "seiscentos": 600, "setecentos": 700, "oitocentos": 800, "novecentos": 900, "mil": 1000
}

def filter_false_positives(mentions):
    """Filter out common false positives from extracted mentions"""
    filtered_mentions = []
    
    for mention in mentions:
        sentence = mention.get("sentence", "").lower()
        entity_text = mention.get("entity_text", "").lower()
        entity_value = mention.get("entity_value", 0)
        
        # Skip if no value or sentence
        if entity_value <= 0 or not sentence:
            continue
        
        # Common false positive patterns
        skip_patterns = [
            r"\d+\s*(graus|°c|\bºc\b|\bc\b)",  # Temperature
            r"\d+\s*(km/h|\bkm\b)",  # Speed/Distance
            r"\d+\s*(mil|milhões|milhares)",  # Large numbers often not victims
            r"\d+\s*(euros|\beur\b|\€)",  # Money
            r"anos \d+",  # Years/dates
            r"\d+\s*anos",  # Age references that aren't victims
            r"década de \d+",  # Decade references
            r"século \d+",  # Century references
        ]
        
        # Check if sentence contains any skip patterns
        should_skip = any(re.search(pattern, sentence) for pattern in skip_patterns)
        
        # Skip dates that are often false positives
        date_indicators = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", 
                          "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
                          
        has_date_indicator = any(date in sentence for date in date_indicators)
        
        # Skip if it's non-victim context
        if should_skip and not re.search(
            r"(\d+)\s*(vítimas|mortos|mortes|feridos|desalojados|evacuados|desaparecidos)", sentence):
            continue
        
        # Skip date references unless they're clearly about victims
        if has_date_indicator and not re.search(
            r"(morrer|falecer|ferir|desalojar|evacuar|desaparecer)\s+\d+", sentence):
            continue
            
        # Skip very high numbers that are unlikely to be correct victim counts for local disasters
        if entity_value > 500 and "milhares" not in sentence and "milhões" not in sentence:
            continue
            
        # Keep this mention as it passed all filters
        filtered_mentions.append(mention)
    
    return filtered_mentions


def analyze_complex_sentence(sentence, entity_type=None):
    """
    Analyze complex sentences to extract multiple potential victim counts
    from different contexts within the same sentence
    """
    results = []
    sentence = sentence.lower()
    
    # Complex patterns for different victim types
    fatalities_patterns = [
        r"(\d+)\s+pessoa[s]?\s+morreram",
        r"(\d+)\s+mortos",
        r"(\d+)\s+vítimas\s+mortais",
        r"(\d+)\s+óbitos",
        r"morreram\s+(\d+)",
        r"faleceram\s+(\d+)"
    ]
    
    injured_patterns = [
        r"(\d+)\s+pessoa[s]?\s+ferida[s]?",
        r"(\d+)\s+feridos",
        r"ficaram\s+feridas\s+(\d+)",
        r"(\d+)\s+vítimas\s+com\s+ferimentos"
    ]
    
    displaced_patterns = [
        r"(\d+)\s+pessoa[s]?\s+desalojada[s]?",
        r"(\d+)\s+desalojados",
        r"(\d+)\s+pessoa[s]?\s+ficaram\s+sem\s+casa"
    ]
    
    evacuated_patterns = [
        r"(\d+)\s+pessoa[s]?\s+evacuada[s]?",
        r"(\d+)\s+evacuados",
        r"evacuaram\s+(\d+)"
    ]
    
    missing_patterns = [
        r"(\d+)\s+pessoa[s]?\s+desaparecida[s]?",
        r"(\d+)\s+desaparecidos",
        r"há\s+(\d+)\s+desaparecidos"
    ]
    
    # Choose patterns based on entity type or use all
    patterns_to_check = []
    if entity_type == "FATALITIES" or not entity_type:
        patterns_to_check.extend([(p, "FATALITIES") for p in fatalities_patterns])
    if entity_type == "INJURED" or not entity_type:
        patterns_to_check.extend([(p, "INJURED") for p in injured_patterns])
    if entity_type == "DISPLACED" or not entity_type:
        patterns_to_check.extend([(p, "DISPLACED") for p in displaced_patterns])
    if entity_type == "EVACUATED" or not entity_type:
        patterns_to_check.extend([(p, "EVACUATED") for p in evacuated_patterns])
    if entity_type == "MISSING" or not entity_type:
        patterns_to_check.extend([(p, "MISSING") for p in missing_patterns])
    
    # Check each pattern
    for pattern, label in patterns_to_check:
        matches = re.finditer(pattern, sentence)
        for match in matches:
            groups = match.groups()
            for group in groups:
                if group and group.isdigit():
                    number = int(group)
                    if 0 < number < 1000:  # Reasonable value check
                        results.append({
                            "entity_label": label,
                            "entity_text": match.group(0),
                            "entity_value": number
                        })
    
    return results

def fetch_and_extract_article_text(url):
    try:
        resp = requests.get(url, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        paragraphs = [p.get_text() for p in soup.find_all("p")]
        return "\n".join(paragraphs).strip()
    except Exception as e:
        print(f"⚠️ Erro ao extrair texto de {url}: {e}")
        return ""

def extract_number_from_text(text, context_window=100):
    """Extract number with surrounding context validation using improved patterns"""
    text = text.lower()
    
    # First try to extract specific victim number patterns with higher precision
    precise_patterns = [
        # Patterns with number followed by victim terms
        r"(\d+)\s+(mortos|mortas|vítimas\s+mortais|óbitos|falecidos)",
        r"(\d+)\s+(feridos|feridas|pessoas\s+feridas)",
        r"(\d+)\s+(desalojados|desalojadas|pessoas\s+desalojadas)",
        r"(\d+)\s+(evacuados|evacuadas|pessoas\s+evacuadas)",
        r"(\d+)\s+(desaparecidos|desaparecidas|pessoas\s+desaparecidas)",
        # Patterns with verb constructions
        r"(morreram|faleceram)\s+(\d+)\s+(pessoas|indivíduos|vítimas)",
        r"(ficaram\s+feridas|sofreram\s+ferimentos)\s+(\d+)\s+(pessoas|indivíduos)",
        r"(houve|há|havendo)\s+(\d+)\s+(mortos|feridos|desalojados|evacuados|desaparecidos)",
        r"(confirmou|confirmada[s]?|registr\w+)\s+(\d+)\s+(mortes|mortos|vítimas)"
    ]
    
    for pattern in precise_patterns:
        match = re.search(pattern, text)
        if match:
            groups = match.groups()
            # Extract the actual number from the match
            number_str = next((g for g in groups if g and g.isdigit()), None)
            if number_str:
                number = int(number_str)
                # Apply realistic upper limit validation
                if number < 1000:  # Reasonable limit for local disasters
                    return number
    
    # Fallback to general number with context validation
    matches = list(re.finditer(r"\d+", text))
    for match in matches:
        number = int(match.group())
        
        # Skip very large numbers or years (likely not victim counts)
        if number > 1000 or (1900 < number < 2100):
            continue
            
        # Get surrounding context with larger window
        start = max(0, match.start() - context_window)
        end = min(len(text), match.end() + context_window)
        context = text[start:end]
        
        # Expanded victim indicators with variations and synonyms
        victim_indicators = [
            # Deaths
            "morto", "morta", "mortos", "mortas", "falecido", "falecida", "falecidos", 
            "vítima mortal", "vítimas mortais", "óbito", "óbitos", "morte", "mortes",
            # Injured
            "ferido", "ferida", "feridos", "feridas", "lesionado", "lesionada", "lesão", "lesões",
            "hospitalizado", "hospitalizada", "ferimento", "ferimentos", "traumatismo",
            # Displaced
            "desalojado", "desalojada", "desalojados", "desalojadas", "sem abrigo", 
            "perderam casa", "perderam suas casas", "ficaram sem casa",
            # Evacuated
            "evacuado", "evacuada", "evacuados", "evacuadas", "retirado", "retirada", "retirados",
            "saíram de casa", "tiveram de abandonar", "obrigados a sair",
            # Missing
            "desaparecido", "desaparecida", "desaparecidos", "desaparecidas", "não localizado"
        ]
        
        # Check if any victim indicator is near the number AND not in a negation context
        negation_terms = ["não", "nenhum", "nenhuma", "sem", "zero"]
        has_victim_term = any(indicator in context for indicator in victim_indicators)
        is_negated = any(neg in context[max(0, match.start() - 20):match.start()] for neg in negation_terms)
        
        if has_victim_term and not is_negated:
            # Additional validation: check for DATE contexts that might be confused
            date_indicators = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", 
                              "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
                              "dia", "mês", "ano", "semana"]
            if not any(date_term in context[max(0, match.start() - 15):match.end() + 15] for date_term in date_indicators):
                return number
    
    # Try word numbers as fallback with better context
    word_match = re.search(r"(um|uma|dois|duas|três|quatro|cinco|seis|sete|oito|nove|dez|onze|doze|treze|catorze|quinze|" +
                        r"dezasseis|dezessete|dezoito|dezanove|vinte)\s+(mortos|mortas|feridos|feridas|desalojados|evacuados|desaparecidos)", text)
    if word_match:
        word = word_match.group(1)
        if word in PT_NUMBER_WORDS:
            return PT_NUMBER_WORDS[word]
    
    # Last fallback for individual word numbers
    words = re.findall(r"\b\w+\b", text.lower())
    for i, word in enumerate(words):
        if word in PT_NUMBER_WORDS and i < len(words) - 1:
            next_word = words[i+1]
            victim_word_patterns = ["morto", "morta", "ferido", "ferida", "desalojado", "evacuado", "desaparecido"]
            if any(next_word.startswith(pattern) for pattern in victim_word_patterns):
                return PT_NUMBER_WORDS[word]
            
    return 0  # Return 0 when no valid number found

def extract_mentions(article_row, ner):
    mentions = []
    texto = article_row.get("texto", None)
    if not texto or texto == "" or pd.isna(texto):
        url = article_row.get("page", "")
        texto = fetch_and_extract_article_text(url)
    if not texto:
        return mentions
        
    # Process the whole article text
    doc = ner(texto)
    
    # Track which sentences we've already processed to avoid duplicates
    processed_sentences = set()
    
    # First pass: extract entities from NER with higher confidence
    for ent in doc.ents:
        if ent.label_ in {"FATALITIES", "INJURED", "EVACUATED", "DISPLACED", "MISSING"}:
            # Get the full sentence for better context
            sent = ent.sent.text if hasattr(ent, "sent") else ""
            sent_key = sent.strip().lower()[:100]  # Create a key to identify this sentence
            
            if sent_key in processed_sentences:
                continue
                
            processed_sentences.add(sent_key)
            
            # Extract number with improved function using the full sentence for context
            value = extract_number_from_text(sent)
            
            # Only add mentions that actually have a numeric value
            if value > 0:
                mention = {
                    "ID": article_row.get("ID"),
                    "date": article_row.get("date"),
                    "type": article_row.get("type"),
                    "subtype": article_row.get("subtype"),
                    "district": article_row.get("district"),
                    "municipali": article_row.get("municipali"),
                    "parish": article_row.get("parish"),
                    "page": article_row.get("page"),
                    "entity_label": ent.label_,
                    "entity_text": ent.text,
                    "entity_value": value,
                    "sentence": sent,
                    "confidence": "high"
                }
                mentions.append(mention)
    
    # Try deep analysis if no mentions were found with high confidence or if we want to find more mentions
    # Split text into sentences for more targeted analysis
    sentences = [sent.text for sent in doc.sents]
    
    # Look for victim-related sentences that might have been missed by NER
    victim_keywords = [
        "morto", "morta", "mortos", "mortas", "falecido", "vítima", "óbito", "morreu",
        "ferido", "ferida", "feridos", "feridas", "lesionado", "lesão",
        "desalojado", "desalojada", "sem casa", "perderam casa",
        "evacuado", "evacuada", "retirado", "retirada", 
        "desaparecido", "desaparecida", "não localizado"
    ]
    
    for sent in sentences:
        sent_lower = sent.lower()
        sent_key = sent_lower[:100]
        
        if sent_key in processed_sentences:
            continue
            
        # Check if this sentence might have victim information
        if any(keyword in sent_lower for keyword in victim_keywords):
            processed_sentences.add(sent_key)
            
            # First try to analyze as a complex sentence that might have multiple victim mentions
            complex_results = analyze_complex_sentence(sent)
            
            if complex_results:
                # Add each result as a separate mention
                for result in complex_results:
                    mention = {
                        "ID": article_row.get("ID"),
                        "date": article_row.get("date"),
                        "type": article_row.get("type"),
                        "subtype": article_row.get("subtype"),
                        "district": article_row.get("district"),
                        "municipali": article_row.get("municipali"),
                        "parish": article_row.get("parish"),
                        "page": article_row.get("page"),
                        "entity_label": result["entity_label"],
                        "entity_text": result["entity_text"],
                        "entity_value": result["entity_value"],
                        "sentence": sent,
                        "confidence": "high"
                    }
                    mentions.append(mention)
            else:
                # Fall back to basic victim type detection
                victim_type = "FATALITIES"  # Default
                if any(k in sent_lower for k in ["ferido", "ferida", "feridos", "feridas", "lesionado", "lesão"]):
                    victim_type = "INJURED"
                elif any(k in sent_lower for k in ["desalojado", "desalojada", "sem casa", "perderam casa"]):
                    victim_type = "DISPLACED"
                elif any(k in sent_lower for k in ["evacuado", "evacuada", "retirado", "retirada"]):
                    victim_type = "EVACUATED"
                elif any(k in sent_lower for k in ["desaparecido", "desaparecida", "não localizado"]):
                    victim_type = "MISSING"
                
                # Try extraction with the more advanced function
                value = extract_number_from_text(sent)
                if value > 0:
                    # Find the relevant entity text
                    match = re.search(r"\d+\s+\w+", sent)
                    entity_text = match.group(0) if match else f"{value}"
                    
                    mention = {
                        "ID": article_row.get("ID"),
                        "date": article_row.get("date"),
                        "type": article_row.get("type"),
                        "subtype": article_row.get("subtype"),
                        "district": article_row.get("district"),
                        "municipali": article_row.get("municipali"),
                        "parish": article_row.get("parish"),
                        "page": article_row.get("page"),
                        "entity_label": victim_type,
                        "entity_text": entity_text,
                        "entity_value": value,
                        "sentence": sent,
                        "confidence": "medium"
                    }
                    mentions.append(mention)
    
    return mentions

def main():
    start_time = time.time()
    print(f"🔎 Carregando modelo NER de vítimas de: {MODEL_PATH}")
    try:
        ner = spacy.load(MODEL_PATH)
        # Ensure sentence boundaries are set
        if not ner.has_pipe("senter") and not ner.has_pipe("parser") and not ner.has_pipe("sentencizer"):
            ner.add_pipe("sentencizer")
    except Exception as e:
        print(f"❌ Não foi possível carregar o modelo: {e}")
        return

    # Load the articles data
    try:
        df = pd.read_csv(INPUT_CSV)
        total = len(df)
        print(f"📄 Total de artigos a processar: {total}")
    except Exception as e:
        print(f"❌ Erro ao carregar arquivo CSV: {e}")
        return

    all_mentions = []
    errors = 0
    articles_with_mentions = set()

    def process_row(idx_row):
        idx, row = idx_row
        article_id = row.get("ID", "")
        url = row.get("page", "")
        
        print(f"➡️ [{idx+1}/{total}] Processando artigo ID={article_id} URL={url}")
        
        try:
            mentions = extract_mentions(row, ner)
            if mentions:
                print(f"   ✓ {len(mentions)} menções extraídas.")
                return mentions, 0, 1 if len(mentions) > 0 else 0
            else:
                print(f"   - Nenhuma menção encontrada.")
                return [], 0, 0
        except Exception as e:
            print(f"   ⚠️ Erro ao processar artigo ID={article_id}: {e}")
            return [], 1, 0

    # Process articles with reasonable number of workers (too many might cause issues)
    cpu_count = os.cpu_count() or 4  # Default to 4 if os.cpu_count() returns None
    max_workers = min(64, cpu_count * 2)
    print(f"🔄 Processando com {max_workers} threads paralelas...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_row, [(i, row) for i, row in df.iterrows()]))
        
    for mentions, error_count, has_mention in results:
        all_mentions.extend(mentions)
        errors += error_count
        if has_mention:
            for m in mentions:
                articles_with_mentions.add(m.get("ID", ""))
    
    # Generate statistics
    total_articles_with_mentions = len(articles_with_mentions)
    total_mentions = len(all_mentions)
    
    if not all_mentions:
        print("⚠️ Nenhuma menção de vítima extraída.")
        return

    # Save results to CSV
    mentions_df = pd.DataFrame(all_mentions)
    mentions_df.to_csv(OUTPUT_CSV, index=False)
    
    # Group by victim type and count
    victim_counts = mentions_df.groupby("entity_label").size()
    
    # Print summary
    print("\n📊 RESUMO DA EXTRAÇÃO:")
    print(f"✅ Total de artigos processados: {total}")
    print(f"✅ Artigos com menções de vítimas: {total_articles_with_mentions} ({(total_articles_with_mentions/total)*100:.1f}%)")
    print(f"✅ Total de menções extraídas: {total_mentions}")
    print(f"⚠️ Erros durante o processamento: {errors}")
    print("\nTipos de vítimas encontradas:")
    for label, count in victim_counts.items():
        print(f"  - {label}: {count} menções")
        
    # Calculate average per article
    print(f"\n📈 Média de {total_mentions/total_articles_with_mentions:.2f} menções por artigo com vítimas")
    
    # Total execution time
    elapsed_time = time.time() - start_time
    print(f"\n⏱️ Tempo total de execução: {elapsed_time:.2f} segundos")
    
    print(f"\n✅ Lista de vítimas extraídas (NER) salva em: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
