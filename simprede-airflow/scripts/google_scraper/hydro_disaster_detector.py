import re
import pickle
from pathlib import Path
from typing import Tuple, Dict, List
import os

class HydromorphologicalDetector:
    """Specialized detector for hydromorphological events (water and geomorphological hazards)"""
    
    def __init__(self):
        self.load_patterns()
        self._compile_patterns()
    
    def load_patterns(self):
        """Load hydromorphological patterns"""
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            patterns_path = Path(script_dir) / "hydro_patterns.pkl"
            
            if patterns_path.exists():
                with open(patterns_path, 'rb') as f:
                    self.patterns = pickle.load(f)
                print(f"✅ Loaded hydromorphological patterns from {patterns_path}")
            else:
                self.patterns = self._default_patterns()
                print("⚠️ Using default hydromorphological patterns")
        except Exception as e:
            print(f"⚠️ Error loading patterns: {e}, using defaults")
            self.patterns = self._default_patterns()
    
    def _default_patterns(self):
        """Default hydromorphological event patterns"""
        return {
            "inundacao_fluvial": {
                "primary_keywords": ["rio", "ribeira", "ribeiro", "cheia", "transborda", "galga"],
                "secondary_keywords": ["curso água", "leito", "margens", "caudal", "afluente"],
                "impact_keywords": ["inunda", "alaga", "evacuação", "desalojados", "isolados"]
            },
            "inundacao_pluvial": {
                "primary_keywords": ["chuva", "precipitação", "aguaceiro", "temporal"],
                "secondary_keywords": ["torrencial", "intensa", "abundante", "drenagem"],
                "impact_keywords": ["alaga", "inunda", "caves", "túneis", "sistema drenagem"]
            },
            "inundacao_costeira": {
                "primary_keywords": ["mar", "maré", "ondas", "ressaca", "tsunami"],
                "secondary_keywords": ["costeira", "marginal", "paredão", "molhe", "agitação marítima"],
                "impact_keywords": ["galga", "invade", "inunda", "galgamento", "evacuação"]
            },
            "deslizamento": {
                "primary_keywords": ["deslizamento", "derrocada", "escorregamento", "deslize"],
                "secondary_keywords": ["encosta", "vertente", "talude", "massa rochosa", "terrenos"],
                "impact_keywords": ["mata", "soterra", "corta estrada", "destrói", "vítimas"]
            },
            "erosao": {
                "primary_keywords": ["erosão", "galgamento", "recuo", "desgaste"],
                "secondary_keywords": ["costeira", "falésia", "margem", "praia", "costa"],
                "impact_keywords": ["destrói", "ameaça", "avança", "come", "recua"]
            },
            "subsidencia": {
                "primary_keywords": ["subsidência", "abatimento", "afundamento", "colapso"],
                "secondary_keywords": ["terreno", "solo", "cavidade", "cratera"],
                "impact_keywords": ["engole", "cede", "afunda", "desaba", "provoca"]
            }
        }
    
    def _compile_patterns(self):
        """Compile regex patterns for efficient matching"""
        self.compiled_patterns = {}
        
        for event_type, pattern_groups in self.patterns.items():
            # Create weighted patterns
            primary_pattern = r'\b(?:' + '|'.join(re.escape(kw) for kw in pattern_groups["primary_keywords"]) + r')\b'
            secondary_pattern = r'\b(?:' + '|'.join(re.escape(kw) for kw in pattern_groups["secondary_keywords"]) + r')\b'
            impact_pattern = r'\b(?:' + '|'.join(re.escape(kw) for kw in pattern_groups["impact_keywords"]) + r')\b'
            
            self.compiled_patterns[event_type] = {
                'primary': re.compile(primary_pattern, re.IGNORECASE),
                'secondary': re.compile(secondary_pattern, re.IGNORECASE),
                'impact': re.compile(impact_pattern, re.IGNORECASE)
            }
    
    def detect_hydro_event(self, text: str) -> Tuple[str, str, float]:
        """
        Detect hydromorphological event type from text
        Returns: (main_type, subtype, confidence_score)
        """
        if not text:
            return "unknown", "", 0.0
        
        text_lower = text.lower()
        scores = {}
        
        # Score each hydromorphological event type
        for event_type, patterns in self.compiled_patterns.items():
            # Count matches for each pattern type with different weights
            primary_matches = len(patterns['primary'].findall(text_lower))
            secondary_matches = len(patterns['secondary'].findall(text_lower))
            impact_matches = len(patterns['impact'].findall(text_lower))
            
            # Weighted scoring: primary keywords are most important
            score = (primary_matches * 3.0) + (secondary_matches * 1.5) + (impact_matches * 2.0)
            
            # Bonus for having matches in multiple categories
            categories_with_matches = sum([
                primary_matches > 0,
                secondary_matches > 0,
                impact_matches > 0
            ])
            
            if categories_with_matches >= 2:
                score *= 1.5  # 50% bonus for multi-category matches
            
            if score > 0:
                scores[event_type] = score
        
        if not scores:
            return "unknown", "", 0.0
        
        # Get the highest scoring event type
        best_event = max(scores.items(), key=lambda x: x[1])
        event_type, score = best_event
        
        # Calculate confidence (normalize score to 0-1 range)
        max_possible_score = 6.0 * 1.5  # 3 primary + 1.5 secondary + 2 impact, with bonus
        confidence = min(score / max_possible_score, 1.0)
        
        # Minimum confidence threshold
        if confidence < 0.3:
            return "unknown", "", confidence
        
        # Determine subtype based on specific context
        subtype = self._determine_hydro_subtype(text_lower, event_type)
        
        return event_type, subtype, confidence
    
    def _determine_hydro_subtype(self, text: str, main_type: str) -> str:
        """Determine specific subtype for hydromorphological events"""
        
        subtype_patterns = {
            "inundacao_fluvial": {
                "repentina": ["súbita", "repentina", "flash", "rápida"],
                "lenta": ["lenta", "progressiva", "gradual"],
                "urbana": ["cidade", "urbana", "centro", "ruas"]
            },
            "inundacao_pluvial": {
                "urbana": ["cidade", "urbana", "ruas", "centro", "túnel"],
                "rural": ["campo", "rural", "agrícola", "campos"]
            },
            "inundacao_costeira": {
                "tempestade": ["tempestade", "temporal", "ciclone"],
                "mare": ["maré", "viva", "sizígia"],
                "tsunami": ["tsunami", "maremoto", "sismo"]
            },
            "deslizamento": {
                "rochoso": ["rocha", "rochoso", "pedra", "bloco"],
                "terras": ["terra", "solo", "lama"],
                "detritos": ["detritos", "destroços", "materiais"]
            },
            "erosao": {
                "costeira": ["costa", "costeira", "praia", "falésia"],
                "fluvial": ["rio", "ribeira", "fluvial", "margem"],
                "pluvial": ["chuva", "pluvial", "escorrência"]
            },
            "subsidencia": {
                "natural": ["natural", "cavidade", "calcário"],
                "antropica": ["obra", "escavação", "túnel", "mina"]
            }
        }
        
        if main_type in subtype_patterns:
            for subtype, patterns in subtype_patterns[main_type].items():
                if any(pattern in text for pattern in patterns):
                    return subtype
        
        return ""
    
    def extract_hydro_impacts(self, text: str) -> Dict[str, int]:
        """Extract impact information specifically for hydromorphological events"""
        impacts = {
            "fatalities": 0,
            "injured": 0,
            "evacuated": 0,
            "displaced": 0,
            "missing": 0,
            "houses_destroyed": 0,
            "houses_damaged": 0,
            "roads_cut": 0
        }
        
        # Hydromorphological-specific impact patterns
        impact_patterns = {
            "fatalities": [
                r'(\d+)\s*(?:mort[oa]s?|vítimas?\s*mortais?|óbitos?|afogad[oa]s?)',
                r'(?:mata|matou|arrastou|soterrou)\s*(\d+)',
                r'(\d+)\s*pessoa[s]?\s*(?:morr[eu]|afog[ou]|soterrad[ao]s?)',
            ],
            "evacuated": [
                r'(\d+)\s*evacuad[oa]s?',
                r'(\d+)\s*pessoa[s]?\s*retirad[ao]s?',
                r'evacuação\s*de\s*(\d+)',
                r'(?:retirou|resgatou)\s*(\d+)',
            ],
            "displaced": [
                r'(\d+)\s*desalojad[oa]s?',
                r'(\d+)\s*(?:sem\s*casa|sem\s*abrigo)',
                r'(\d+)\s*famílias?\s*afetad[ao]s?',
            ],
            "houses_destroyed": [
                r'(\d+)\s*casas?\s*(?:destruíd[ao]s?|demolid[ao]s?|arrasad[ao]s?)',
                r'(\d+)\s*habitações?\s*destruíd[ao]s?',
                r'(?:destruiu|derrubou)\s*(\d+)\s*casas?',
            ],
            "houses_damaged": [
                r'(\d+)\s*casas?\s*(?:danificad[ao]s?|afetad[ao]s?)',
                r'(\d+)\s*habitações?\s*(?:danificad[ao]s?|inundad[ao]s?)',
                r'(?:danificou|afetou)\s*(\d+)\s*casas?',
            ],
            "roads_cut": [
                r'(\d+)\s*(?:estradas?|vias?)\s*(?:cortad[ao]s?|intransitávei[s]?|fechad[ao]s?)',
                r'(?:cortou|fechou|bloqueou)\s*(\d+)\s*estradas?',
                r'(\d+)\s*acessos?\s*cortad[oa]s?',
            ]
        }
        
        text_lower = text.lower()
        
        # Number word mappings for Portuguese
        num_words = {
            "um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "tres": 3,
            "quatro": 4, "cinco": 5, "seis": 6, "sete": 7, "oito": 8,
            "nove": 9, "dez": 10, "dezena": 10, "dezenas": 20, "dúzia": 12,
            "centena": 100, "várias": 5, "alguns": 3, "muitos": 10
        }
        
        for impact_type, patterns in impact_patterns.items():
            for pattern in patterns:
                matches = re.finditer(pattern, text_lower)
                for match in matches:
                    try:
                        num = int(match.group(1))
                        impacts[impact_type] = max(impacts[impact_type], num)
                    except (ValueError, IndexError):
                        # Try word-to-number conversion
                        try:
                            word = match.group(1).lower()
                            if word in num_words:
                                impacts[impact_type] = max(impacts[impact_type], num_words[word])
                        except IndexError:
                            pass
        
        return impacts
    
    def is_hydromorphological_event(self, text: str, threshold: float = 0.3) -> bool:
        """Check if text describes a hydromorphological event"""
        event_type, _, confidence = self.detect_hydro_event(text)
        return event_type != "unknown" and confidence >= threshold

# Usage example and testing
if __name__ == "__main__":
    detector = HydromorphologicalDetector()
    
    test_texts = [
        "Rio Tejo transborda e inunda centro de Santarém evacuando 50 pessoas",
        "Chuva torrencial causa alagamentos no centro de Lisboa fechando túneis",
        "Mar galga paredão da marginal e inunda estrada em Cascais",
        "Deslizamento de terras mata duas pessoas e destrói cinco casas",
        "Erosão costeira ameaça destruir casas na primeira linha de Aveiro", 
        "Terreno abate e engole casa em Sintra criando cratera de 10 metros",
        "Incêndio florestal combatido por bombeiros e meios aéreos",  # Non-hydro
        "Jogo de futebol entre Benfica e Porto termina empatado"  # Non-disaster
    ]
    
    print("🧪 Testing Hydromorphological Event Detection:")
    print("=" * 60)
    
    for text in test_texts:
        event_type, subtype, confidence = detector.detect_hydro_event(text)
        impacts = detector.extract_hydro_impacts(text)
        is_hydro = detector.is_hydromorphological_event(text)
        
        print(f"Text: {text}")
        print(f"  Event Type: {event_type}")
        print(f"  Subtype: {subtype}")
        print(f"  Confidence: {confidence:.3f}")
        print(f"  Is Hydromorphological: {is_hydro}")
        if any(impacts.values()):
            print(f"  Impacts: {[(k, v) for k, v in impacts.items() if v > 0]}")
        print()