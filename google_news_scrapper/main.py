import argparse
import os
import subprocess
from datetime import datetime
import sys

def validate_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        print(f"⚠️ Formato de data inválido: {date_str}. Use YYYY-MM-DD")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Pipeline de scraping e processamento de notícias")
    parser.add_argument(
        "--etapa",
        choices=[
            "run_scraper",
            "classificacao",
            "train_classifier",
            "classificar_artigos",
            "processar_relevantes",
            "filtrar_artigos_vitimas",
            "train_victim_ner",
            "extract_victims_with_ner",
            "exportar_bd_artigos_google",
            "exportar_bd_artigos_municipios",
            "supabase_export_script",
            "all"
        ],
        default="all",
        help="Escolhe a etapa do pipeline ou executa todas as etapas ('all').",
    )
    parser.add_argument(
        "--date",
        help="Data específica para coletar notícias (formato: YYYY-MM-DD)",
        type=validate_date
    )
    args = parser.parse_args()

    # Always ask for date if not provided
    if not args.date:
        date_input = input("📅 Digite a data para processar (YYYY-MM-DD): ")
        args.date = validate_date(date_input)
        if not args.date:
            print("❌ Data é obrigatória para o processamento!")
            sys.exit(1)

    print(f"📅 Processando data: {args.date}")

    scripts = {
        "run_scraper": "scraping/run_scraper.py",
        "classificacao": "classificador/classificacao.py",
        "train_classifier": "classificador/train_classifier.py",
        "classificar_artigos": "classificador/classificar_artigos_relevantes_ml.py",
        "processar_relevantes": "processador/processar_relevantes.py",
        "train_victim_ner": "nlp/train_victim_ner.py",  
        "extract_victims_with_ner": "nlp/extract_victims_with_ner.py",
        "filtrar_artigos_vitimas": "processador/filtrar_artigos_vitimas.py",
        "exportar_bd_artigos_municipios": "exportador_bd/supabase_export_script_artigos_municipios_pt.py",
        "supabase_export_script.py": "exportador_bd/supabase_export_script.py",
    }

    # Define which scripts should receive the date parameter
    date_dependent_scripts = [
        "run_scraper",
        "classificar_artigos",
        "processar_relevantes",
        "filtrar_artigos_vitimas"
    ]

    if args.etapa == "all":
        for etapa, script in scripts.items():
            print(f"🚀 Executando etapa: {etapa}...")
            cmd = ["python3", os.path.join(os.path.dirname(__file__), script)]
            if args.date and etapa in date_dependent_scripts:
                cmd.extend(["--date", args.date])
            subprocess.run(cmd, check=True)
    else:
        script = scripts[args.etapa]
        cmd = ["python3", os.path.join(os.path.dirname(__file__), script)]
        if args.date and args.etapa in date_dependent_scripts:
            cmd.extend(["--date", args.date])
        print(f"🚀 Executando etapa: {args.etapa}...")
        subprocess.run(cmd, check=True)

if __name__ == "__main__":
    main()
