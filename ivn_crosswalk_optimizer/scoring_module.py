"""
IVN Enhanced Crosswalk - Scoring Module
=======================================

Multi-dimensional scoring system that combines:
- Text similarity (regex/fuzzy matching)
- Embedding similarity (semantic vectors)
- Enabling relationship scores
- Master table confidence

Provides bucketing logic for alignment classification.
"""

import re
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import logging
import json

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    from difflib import SequenceMatcher

try:
    from sentence_transformers import SentenceTransformer
    import torch
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    EMBEDDINGS_AVAILABLE = False

logger = logging.getLogger(__name__)


class AlignmentBucket(Enum):
    """Classification buckets for alignment results."""
    CONFIRMED = "Confirmed"      # High confidence, all validations pass
    REVIEW = "Review_Needed"     # Medium confidence or validation issues
    REJECTED = "Rejected"        # Low confidence or failed validations
    ORPHAN = "Orphan"           # No suitable alignment found


@dataclass
class ScoringWeights:
    """Weights for multi-dimensional scoring."""
    text_similarity: float = 0.2
    embedding_similarity: float = 0.3
    enabling_score: float = 0.4
    master_confidence: float = 0.1
    
    def __post_init__(self):
        """Validate weights sum to 1.0."""
        total = (self.text_similarity + self.embedding_similarity + 
                 self.enabling_score + self.master_confidence)
        if not (0.99 <= total <= 1.01):
            logger.warning(f"Scoring weights sum to {total}, not 1.0. Normalizing...")
            self.text_similarity /= total
            self.embedding_similarity /= total
            self.enabling_score /= total
            self.master_confidence /= total


@dataclass
class ScoringThresholds:
    """Thresholds for alignment classification."""
    confirmed: float = 0.8      # >= this AND all validations pass -> CONFIRMED
    review: float = 0.6         # >= this but < confirmed OR validation issues -> REVIEW
    # < review -> REJECTED/ORPHAN


@dataclass
class ComponentScores:
    """Container for all scoring dimensions."""
    text_similarity: float = 0.0
    embedding_similarity: float = 0.0
    enabling_score: float = 0.0
    master_confidence: float = 0.0
    final_score: float = 0.0
    bucket: AlignmentBucket = AlignmentBucket.ORPHAN
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'text_similarity': self.text_similarity,
            'embedding_similarity': self.embedding_similarity,
            'enabling_score': self.enabling_score,
            'master_confidence': self.master_confidence,
            'final_score': self.final_score,
            'bucket': self.bucket.value
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())


class TextSimilarityScorer:
    """
    Calculates text similarity using fuzzy matching and regex patterns.
    """
    
    def __init__(self):
        """Initialize the text similarity scorer."""
        self.use_rapidfuzz = RAPIDFUZZ_AVAILABLE
        if not self.use_rapidfuzz:
            logger.warning("rapidfuzz not available, using difflib (slower)")
    
    def normalize_text(self, text: str) -> str:
        """Normalize text for comparison."""
        if not text:
            return ""
        # Lowercase, remove extra whitespace, remove special chars
        text = text.lower().strip()
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[^\w\s]', '', text)
        return text
    
    def calculate_fuzzy_similarity(self, text_a: str, text_b: str) -> float:
        """
        Calculate fuzzy similarity score.
        
        Args:
            text_a: First text
            text_b: Second text
            
        Returns:
            Similarity score 0-1
        """
        norm_a = self.normalize_text(text_a)
        norm_b = self.normalize_text(text_b)
        
        if not norm_a or not norm_b:
            return 0.0
        
        if self.use_rapidfuzz:
            # Use multiple fuzzy metrics and average
            ratio = fuzz.ratio(norm_a, norm_b) / 100.0
            partial = fuzz.partial_ratio(norm_a, norm_b) / 100.0
            token_sort = fuzz.token_sort_ratio(norm_a, norm_b) / 100.0
            token_set = fuzz.token_set_ratio(norm_a, norm_b) / 100.0
            
            # Weighted average favoring token-based metrics
            return (ratio * 0.2 + partial * 0.2 + token_sort * 0.3 + token_set * 0.3)
        else:
            # Fallback to difflib
            return SequenceMatcher(None, norm_a, norm_b).ratio()
    
    def calculate_name_similarity(self, name_a: str, name_b: str) -> float:
        """Calculate similarity between component names."""
        return self.calculate_fuzzy_similarity(name_a, name_b)
    
    def calculate_description_similarity(self, desc_a: str, desc_b: str) -> float:
        """Calculate similarity between component descriptions."""
        return self.calculate_fuzzy_similarity(desc_a, desc_b)
    
    def calculate_combined_similarity(
        self,
        name_a: str,
        name_b: str,
        desc_a: str,
        desc_b: str,
        name_weight: float = 0.4,
        desc_weight: float = 0.6
    ) -> float:
        """
        Calculate combined text similarity from name and description.
        
        Args:
            name_a, name_b: Component names
            desc_a, desc_b: Component descriptions
            name_weight: Weight for name similarity
            desc_weight: Weight for description similarity
            
        Returns:
            Combined similarity score 0-1
        """
        name_sim = self.calculate_name_similarity(name_a, name_b)
        desc_sim = self.calculate_description_similarity(desc_a, desc_b)
        
        return (name_sim * name_weight) + (desc_sim * desc_weight)


class EmbeddingSimilarityScorer:
    """
    Calculates semantic similarity using sentence embeddings.
    """
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2", cache_embeddings: bool = True):
        """
        Initialize the embedding scorer.
        
        Args:
            model_name: SentenceTransformers model name
            cache_embeddings: Whether to cache computed embeddings
        """
        self.model_name = model_name
        self.cache_embeddings = cache_embeddings
        self.embedding_cache: Dict[str, np.ndarray] = {}
        self.model = None
        
        if EMBEDDINGS_AVAILABLE:
            try:
                logger.info(f"Loading embedding model: {model_name}")
                self.model = SentenceTransformer(model_name)
                logger.info("Embedding model loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load embedding model: {e}")
                self.model = None
        else:
            logger.warning("sentence-transformers not available, embedding similarity disabled")
    
    def get_embedding(self, text: str) -> Optional[np.ndarray]:
        """
        Get embedding vector for text.
        
        Args:
            text: Input text
            
        Returns:
            Embedding vector or None if unavailable
        """
        if self.model is None:
            return None
        
        text = str(text).strip()
        if not text:
            return None
        
        # Check cache
        if self.cache_embeddings and text in self.embedding_cache:
            return self.embedding_cache[text]
        
        # Compute embedding
        try:
            embedding = self.model.encode(text, convert_to_numpy=True)
            
            if self.cache_embeddings:
                self.embedding_cache[text] = embedding
            
            return embedding
        except Exception as e:
            logger.error(f"Embedding computation error: {e}")
            return None
    
    def get_embeddings_batch(self, texts: List[str]) -> Dict[str, np.ndarray]:
        """
        Get embeddings for multiple texts efficiently.
        
        Args:
            texts: List of input texts
            
        Returns:
            Dictionary mapping text to embedding
        """
        if self.model is None:
            return {}
        
        # Filter out empty texts and already cached
        texts_to_encode = []
        for text in texts:
            text = str(text).strip()
            if text and (not self.cache_embeddings or text not in self.embedding_cache):
                texts_to_encode.append(text)
        
        if texts_to_encode:
            try:
                embeddings = self.model.encode(texts_to_encode, convert_to_numpy=True, 
                                               show_progress_bar=True)
                
                for text, embedding in zip(texts_to_encode, embeddings):
                    if self.cache_embeddings:
                        self.embedding_cache[text] = embedding
            except Exception as e:
                logger.error(f"Batch embedding error: {e}")
        
        # Return all embeddings
        result = {}
        for text in texts:
            text = str(text).strip()
            if text in self.embedding_cache:
                result[text] = self.embedding_cache[text]
        
        return result
    
    def calculate_similarity(self, text_a: str, text_b: str) -> float:
        """
        Calculate cosine similarity between two texts.
        
        Args:
            text_a: First text
            text_b: Second text
            
        Returns:
            Cosine similarity 0-1 (or 0 if embeddings unavailable)
        """
        emb_a = self.get_embedding(text_a)
        emb_b = self.get_embedding(text_b)
        
        if emb_a is None or emb_b is None:
            return 0.0
        
        # Cosine similarity
        dot_product = np.dot(emb_a, emb_b)
        norm_a = np.linalg.norm(emb_a)
        norm_b = np.linalg.norm(emb_b)
        
        if norm_a == 0 or norm_b == 0:
            return 0.0
        
        similarity = dot_product / (norm_a * norm_b)
        
        # Ensure 0-1 range
        return max(0.0, min(1.0, (similarity + 1) / 2))  # Transform from [-1,1] to [0,1]
    
    def calculate_combined_similarity(
        self,
        name_a: str,
        name_b: str,
        desc_a: str,
        desc_b: str
    ) -> float:
        """
        Calculate combined embedding similarity.
        
        Combines name and description for richer semantic representation.
        """
        combined_a = f"{name_a}. {desc_a}".strip()
        combined_b = f"{name_b}. {desc_b}".strip()
        
        return self.calculate_similarity(combined_a, combined_b)
    
    def clear_cache(self):
        """Clear the embedding cache."""
        self.embedding_cache.clear()
        logger.info("Embedding cache cleared")


class MultiDimensionalScorer:
    """
    Combines multiple scoring dimensions into final alignment scores.
    """
    
    def __init__(
        self,
        weights: ScoringWeights = None,
        thresholds: ScoringThresholds = None,
        embedding_model: str = "all-MiniLM-L6-v2"
    ):
        """
        Initialize the multi-dimensional scorer.
        
        Args:
            weights: Scoring weights configuration
            thresholds: Classification thresholds
            embedding_model: Name of the embedding model to use
        """
        self.weights = weights or ScoringWeights()
        self.thresholds = thresholds or ScoringThresholds()
        
        # Initialize scorers
        self.text_scorer = TextSimilarityScorer()
        self.embedding_scorer = EmbeddingSimilarityScorer(
            model_name=embedding_model,
            cache_embeddings=True
        )
    
    def calculate_scores(
        self,
        component_a: Dict[str, Any],
        component_b: Dict[str, Any],
        enabling_score: float = 0.0,
        master_confidence: float = 1.0,
        all_validations_pass: bool = True
    ) -> ComponentScores:
        """
        Calculate all scoring dimensions for a component pair.
        
        Args:
            component_a: Dict with keys: name, description
            component_b: Dict with keys: name, description
            enabling_score: Pre-calculated enabling relationship score (0-1)
            master_confidence: Master table validation confidence (0-1)
            all_validations_pass: Whether all validation checks passed
            
        Returns:
            ComponentScores with all dimensions and final score
        """
        name_a = str(component_a.get('name', '')).strip()
        name_b = str(component_b.get('name', '')).strip()
        desc_a = str(component_a.get('description', '')).strip()
        desc_b = str(component_b.get('description', '')).strip()
        
        # Calculate text similarity
        text_sim = self.text_scorer.calculate_combined_similarity(
            name_a, name_b, desc_a, desc_b
        )
        
        # Calculate embedding similarity
        embed_sim = self.embedding_scorer.calculate_combined_similarity(
            name_a, name_b, desc_a, desc_b
        )
        
        # Calculate weighted final score
        final_score = (
            text_sim * self.weights.text_similarity +
            embed_sim * self.weights.embedding_similarity +
            enabling_score * self.weights.enabling_score +
            master_confidence * self.weights.master_confidence
        )
        
        # Determine bucket
        bucket = self._determine_bucket(final_score, all_validations_pass)
        
        return ComponentScores(
            text_similarity=text_sim,
            embedding_similarity=embed_sim,
            enabling_score=enabling_score,
            master_confidence=master_confidence,
            final_score=final_score,
            bucket=bucket
        )
    
    def _determine_bucket(self, final_score: float, all_validations_pass: bool) -> AlignmentBucket:
        """
        Determine the alignment bucket based on score and validations.
        
        Args:
            final_score: Combined weighted score
            all_validations_pass: Whether all validation checks passed
            
        Returns:
            AlignmentBucket classification
        """
        if final_score >= self.thresholds.confirmed and all_validations_pass:
            return AlignmentBucket.CONFIRMED
        elif final_score >= self.thresholds.review:
            return AlignmentBucket.REVIEW
        else:
            return AlignmentBucket.REJECTED
    
    def score_candidates_batch(
        self,
        candidates_df: pd.DataFrame,
        name_a_col: str,
        name_b_col: str,
        desc_a_col: str,
        desc_b_col: str,
        enabling_col: str = None,
        master_conf_col: str = None,
        validation_col: str = None
    ) -> pd.DataFrame:
        """
        Score a batch of candidate pairs.
        
        Args:
            candidates_df: DataFrame with candidate pairs
            name_a_col, name_b_col: Column names for component names
            desc_a_col, desc_b_col: Column names for descriptions
            enabling_col: Column name for enabling scores (optional)
            master_conf_col: Column name for master confidence (optional)
            validation_col: Column name for validation status (optional)
            
        Returns:
            DataFrame with scoring columns added
        """
        results = candidates_df.copy()
        
        # Pre-compute embeddings for efficiency
        all_texts = []
        for col in [name_a_col, name_b_col, desc_a_col, desc_b_col]:
            if col in results.columns:
                all_texts.extend(results[col].astype(str).tolist())
        
        # Unique texts only
        unique_texts = list(set([t.strip() for t in all_texts if t.strip()]))
        logger.info(f"Pre-computing embeddings for {len(unique_texts)} unique texts...")
        self.embedding_scorer.get_embeddings_batch(unique_texts)
        
        # Score each pair
        text_sims = []
        embed_sims = []
        final_scores = []
        buckets = []
        
        for idx, row in results.iterrows():
            component_a = {
                'name': row.get(name_a_col, ''),
                'description': row.get(desc_a_col, '')
            }
            component_b = {
                'name': row.get(name_b_col, ''),
                'description': row.get(desc_b_col, '')
            }
            
            enabling = row.get(enabling_col, 0.5) if enabling_col else 0.5
            master_conf = row.get(master_conf_col, 1.0) if master_conf_col else 1.0
            
            # Determine validation status
            if validation_col and validation_col in row:
                val_status = str(row[validation_col]).upper()
                all_pass = val_status in ['VALID', 'TRUE', '1', 'PASSED']
            else:
                all_pass = True
            
            scores = self.calculate_scores(
                component_a, component_b,
                enabling_score=enabling,
                master_confidence=master_conf,
                all_validations_pass=all_pass
            )
            
            text_sims.append(scores.text_similarity)
            embed_sims.append(scores.embedding_similarity)
            final_scores.append(scores.final_score)
            buckets.append(scores.bucket.value)
        
        results['Text_Similarity'] = text_sims
        results['Embedding_Similarity'] = embed_sims
        results['Final_Score'] = final_scores
        results['Alignment_Bucket'] = buckets
        
        return results
    
    def precompute_embeddings(self, components_df: pd.DataFrame, text_columns: List[str]):
        """
        Pre-compute and cache embeddings for all components.
        
        Args:
            components_df: DataFrame with components
            text_columns: List of column names containing text to embed
        """
        all_texts = []
        for col in text_columns:
            if col in components_df.columns:
                all_texts.extend(components_df[col].astype(str).tolist())
        
        unique_texts = list(set([t.strip() for t in all_texts if t.strip()]))
        logger.info(f"Pre-computing embeddings for {len(unique_texts)} unique texts...")
        self.embedding_scorer.get_embeddings_batch(unique_texts)
        logger.info("Embedding pre-computation complete")


def create_scorer_from_config(config: Dict[str, Any]) -> MultiDimensionalScorer:
    """
    Create a MultiDimensionalScorer from configuration dictionary.
    
    Args:
        config: Configuration dictionary with 'weights' and 'thresholds' sections
        
    Returns:
        Configured MultiDimensionalScorer instance
    """
    weights_config = config.get('weights', {})
    thresholds_config = config.get('thresholds', {})
    embeddings_config = config.get('embeddings', {})
    
    weights = ScoringWeights(
        text_similarity=weights_config.get('text_similarity', 0.2),
        embedding_similarity=weights_config.get('embedding_similarity', 0.3),
        enabling_score=weights_config.get('enabling_score', 0.4),
        master_confidence=weights_config.get('master_confidence', 0.1)
    )
    
    thresholds = ScoringThresholds(
        confirmed=thresholds_config.get('final_score', 0.75),
        review=thresholds_config.get('similarity', 0.6)
    )
    
    embedding_model = embeddings_config.get('model_name', 'all-MiniLM-L6-v2')
    
    return MultiDimensionalScorer(
        weights=weights,
        thresholds=thresholds,
        embedding_model=embedding_model
    )
