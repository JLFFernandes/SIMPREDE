#TODO: Comentarios de código em português e preparado para produção
# File: airflow-GOOGLE-NEWS-SCRAPER/scripts/google_scraper/processador/ml_enhanced_filter.py
# Script para aplicar filtro aprimorado de artigos usando modelos de machine learning

import pickle
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import os

class MLEnhancedFilter:
    def __init__(self, models_dir=None):
        if models_dir is None:
            # Auto-detect models directory
            script_dir = os.path.dirname(os.path.abspath(__file__))
            models_dir = os.path.join(os.path.dirname(script_dir), "models")
        
        self.models_dir = Path(models_dir)
        self.vectorizer = None
        self.classifier = None
        self.logger = logging.getLogger(__name__)
        
        # Log the models directory being used
        self.logger.info(f"🔍 ML Enhanced Filter initialized with models directory: {self.models_dir}")
        
    def load_models(self):
        """Load the trained TF-IDF vectorizer and classification model"""
        try:
            # Check if models directory exists
            if not self.models_dir.exists():
                self.logger.warning(f"⚠️ Models directory not found: {self.models_dir}")
                return False
            
            # Load TF-IDF vectorizer
            vectorizer_path = self.models_dir / "tfidf_vectorizer.pkl"
            if not vectorizer_path.exists():
                self.logger.warning(f"⚠️ TF-IDF vectorizer not found at {vectorizer_path}")
                return False
            
            # Check file size and content
            if vectorizer_path.stat().st_size == 0:
                self.logger.warning(f"⚠️ TF-IDF vectorizer file is empty: {vectorizer_path}")
                return False
                
            with open(vectorizer_path, 'rb') as f:
                try:
                    self.vectorizer = pickle.load(f)
                    self.logger.info(f"✅ TF-IDF vectorizer loaded from {vectorizer_path}")
                except (pickle.UnpicklingError, EOFError, ValueError) as e:
                    self.logger.error(f"❌ Failed to unpickle TF-IDF vectorizer: {e}")
                    return False
            
            # Load classification model
            model_path = self.models_dir / "modelo_classificacao.pkl"
            if not model_path.exists():
                self.logger.warning(f"⚠️ Classification model not found at {model_path}")
                return False
            
            # Check file size and content
            if model_path.stat().st_size == 0:
                self.logger.warning(f"⚠️ Classification model file is empty: {model_path}")
                return False
                
            with open(model_path, 'rb') as f:
                try:
                    self.classifier = pickle.load(f)
                    self.logger.info(f"✅ Classification model loaded from {model_path}")
                except (pickle.UnpicklingError, EOFError, ValueError) as e:
                    self.logger.error(f"❌ Failed to unpickle classification model: {e}")
                    return False
                
            self.logger.info("✅ ML models loaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load ML models: {e}")
            import traceback
            self.logger.error(f"❌ Traceback: {traceback.format_exc()}")
            return False
    
    def prepare_text_features(self, articles_df):
        """Prepare text features for ML prediction"""
        text_features = []
        
        for _, row in articles_df.iterrows():
            # Combine available text fields
            text_parts = []
            
            if 'title' in row and pd.notna(row['title']):
                text_parts.append(str(row['title']))
            
            if 'link' in row and pd.notna(row['link']):
                # Extract domain/source information
                from urllib.parse import urlparse
                try:
                    domain = urlparse(str(row['link'])).netloc
                    text_parts.append(domain.replace('www.', ''))
                except:
                    pass
            
            # You could also add article content if available
            if 'description' in row and pd.notna(row['description']):
                text_parts.append(str(row['description'])[:200])  # First 200 chars
            
            combined_text = ' '.join(text_parts)
            text_features.append(combined_text if combined_text.strip() else "no content")
        
        return text_features
    
    def predict_relevance(self, articles_df, threshold=0.5):
        """Predict article relevance using trained models"""
        if not self.vectorizer or not self.classifier:
            self.logger.warning("⚠️ Models not loaded, falling back to no filtering")
            return np.ones(len(articles_df), dtype=bool), np.ones(len(articles_df))
        
        try:
            # Prepare text features
            text_features = self.prepare_text_features(articles_df)
            self.logger.info(f"🔍 Prepared {len(text_features)} text features for ML prediction")
            
            # Transform text using TF-IDF vectorizer
            text_vectors = self.vectorizer.transform(text_features)
            self.logger.info(f"🔍 Transformed text to vectors: {text_vectors.shape}")
            
            # Predict probabilities
            probabilities = self.classifier.predict_proba(text_vectors)
            self.logger.info(f"🔍 Generated predictions: {probabilities.shape}")
            
            # Get probability of positive class (assuming binary classification)
            if probabilities.shape[1] == 2:
                positive_probs = probabilities[:, 1]
            else:
                positive_probs = probabilities.max(axis=1)
            
            # Apply threshold
            predictions = positive_probs >= threshold
            
            self.logger.info(f"🤖 ML filtering: {predictions.sum()}/{len(predictions)} articles predicted as relevant (threshold={threshold})")
            
            return predictions, positive_probs
            
        except Exception as e:
            self.logger.error(f"❌ ML prediction failed: {e}")
            import traceback
            self.logger.error(f"❌ Traceback: {traceback.format_exc()}")
            return np.ones(len(articles_df), dtype=bool), np.ones(len(articles_df))
    
    def enhanced_filter(self, articles_df, ml_threshold=0.5, combine_with_rules=True):
        """Apply ML-enhanced filtering with optional rule-based combination"""
        
        # Get ML predictions
        ml_predictions, ml_scores = self.predict_relevance(articles_df, ml_threshold)
        
        if combine_with_rules:
            # Combine with existing rule-based filters
            rule_based_mask = self.apply_rule_based_filters(articles_df)
            
            # Combine: keep articles that pass either ML or rule-based filters
            combined_mask = ml_predictions | rule_based_mask
            
            self.logger.info(f"🔄 Combined filtering: ML={ml_predictions.sum()}, Rules={rule_based_mask.sum()}, Combined={combined_mask.sum()}")
            
            return combined_mask, ml_scores
        else:
            return ml_predictions, ml_scores
    
    def apply_rule_based_filters(self, articles_df):
        """Apply basic rule-based filters"""
        mask = np.ones(len(articles_df), dtype=bool)
        
        # Content quality filter
        if 'title' in articles_df.columns:
            # Filter out very short titles
            title_quality = articles_df['title'].str.len() >= 10
            mask = mask & title_quality.fillna(False)
        
        # Keywords in title/content
        disaster_keywords = ['incêndio', 'inundação', 'temporal', 'vento', 'chuva', 'neve']
        if 'title' in articles_df.columns:
            title_text = articles_df['title'].fillna('').str.lower()
            has_disaster_keywords = title_text.str.contains('|'.join(disaster_keywords), na=False)
            # Don't exclude based on keywords, just prioritize
        
        return mask