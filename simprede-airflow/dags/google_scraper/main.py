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
            "processar_relevantes",
            "filtrar_artigos_vitimas",
            "export_to_supabase",
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
    parser.add_argument(
        "--dias",
        type=int,
        help="Número de dias anteriores a considerar para as notícias"
    )
    args = parser.parse_args()

    if not args.dias:
        dias_input = input("⏳ Quantos dias anteriores queres considerar para o scraping? (default: 1): ")
        if dias_input.strip().isdigit():
            args.dias = int(dias_input)
        else:
            args.dias = 1

    # Get current date for file naming
    current_date = datetime.now().strftime("%Y%m%d")
    print(f"📅 A processar artigos dos últimos {args.dias} dia(s) com data {current_date}...")

    scripts = {
        "run_scraper": "scraping/run_scraper.py",
        "processar_relevantes": "processador/processar_relevantes.py",
        "filtrar_artigos_vitimas": "processador/filtrar_artigos_vitimas.py",
        "export_to_supabase": "exportador_bd/export_to_supabase.py",
    }

    # All scripts should receive dias parameter
    dias_dependent_scripts = [
        "run_scraper",
        "processar_relevantes",
        "filtrar_artigos_vitimas"
    ]

    if args.etapa == "all":
        for etapa, script in scripts.items():
            print(f"🚀 Executando etapa: {etapa}...")
            cmd = ["python3", os.path.join(os.path.dirname(__file__), script)]
            if etapa in dias_dependent_scripts:
                cmd.extend(["--dias", str(args.dias)])
            print(f"📦 Comando: {' '.join(cmd)}")
            subprocess.run(cmd, check=True)
    else:
        script = scripts[args.etapa]
        cmd = ["python3", os.path.join(os.path.dirname(__file__), script)]
        if args.etapa in dias_dependent_scripts:
            cmd.extend(["--dias", str(args.dias)])
        print(f"🚀 Executando etapa: {args.etapa}...")
        print(f"📦 Comando: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)

    print(f"✅ Processamento completo! Arquivos gerados com a data {current_date}")

if __name__ == "__main__":
    main()
