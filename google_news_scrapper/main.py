import argparse
import os
import subprocess

def main():
    parser = argparse.ArgumentParser(description="Pipeline de scraping e processamento de notícias")
    parser.add_argument(
        "--etapa",
        choices=["run_scraper", "classificacao", "train_classifier", "classificar_artigos", "processar_relevantes", "all"],
        default="all",
        help="Escolhe a etapa do pipeline ou executa todas as etapas ('all').",
    )
    args = parser.parse_args()

    scripts = {
        "run_scraper": "scraping/run_scraper.py",
        "classificacao": "classificador/classificacao.py",
        "train_classifier": "classificador/train_classifier.py",
        "classificar_artigos": "classificador/classificar_artigos_relevantes_ml.py",
        "processar_relevantes": "processador/processar_relevantes.py",
        "filtrar_artigos_vitimas": "processador/filtrar_artigos_vitimas.py",
    }

    if args.etapa == "all":
        for etapa, script in scripts.items():
            print(f"🚀 Executando etapa: {etapa}...")
            subprocess.run(["python3", os.path.join(os.path.dirname(__file__), script)], check=True)
    else:
        script = scripts[args.etapa]
        print(f"🚀 Executando etapa: {args.etapa}...")
        subprocess.run(["python3", os.path.join(os.path.dirname(__file__), script)], check=True)

if __name__ == "__main__":
    main()
