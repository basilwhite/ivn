"""
IVN Enhanced Crosswalk - Validation Module
==========================================

Provides master table validation and semantic relationship validation
for component alignment in IVN governance documents.

Key Functions:
- Master table lookup and validation
- Semantic enabling-relationship analysis
- LLM-based and heuristic validation
"""

import re
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ValidationStatus(Enum):
    """Validation result status codes."""
    VALID = "VALID"
    INVALID = "INVALID"
    REVIEW = "REVIEW"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"


@dataclass
class ValidationResult:
    """Container for validation results."""
    status: ValidationStatus
    confidence: float  # 0-100
    reasoning: str
    details: Dict[str, Any] = field(default_factory=dict)


class MasterTableValidator:
    """
    Validates components against the Component Master Table.
    
    The master table serves as the authoritative source for:
    - Valid component IDs
    - Correct parent deliverable (Valid_Source_ID)
    - Component metadata
    """
    
    def __init__(self, master_df: pd.DataFrame, column_mapping: Dict[str, str]):
        """
        Initialize with master table data.
        
        Args:
            master_df: DataFrame containing master component table
            column_mapping: Dict mapping standard names to actual column names
        """
        self.master_df = master_df
        self.column_mapping = column_mapping
        
        # Build lookup maps
        self._build_lookup_maps()
        
        logger.info(f"MasterTableValidator initialized with {len(self.master_df)} components")
    
    def _build_lookup_maps(self):
        """Build efficient lookup dictionaries from master table."""
        comp_id_col = self.column_mapping.get('component_id', 'Component_ID')
        valid_source_col = self.column_mapping.get('valid_source_id', 'Valid_Source_ID')
        comp_name_col = self.column_mapping.get('component_name', 'Component_Name')
        
        # Component ID -> Valid Source ID mapping
        self.component_to_source_map: Dict[str, str] = {}
        # Component ID -> Component Name mapping
        self.component_to_name_map: Dict[str, str] = {}
        # Set of valid component IDs
        self.valid_component_ids: set = set()
        
        for _, row in self.master_df.iterrows():
            comp_id = str(row.get(comp_id_col, '')).strip()
            if comp_id:
                self.valid_component_ids.add(comp_id)
                self.component_to_source_map[comp_id] = str(row.get(valid_source_col, '')).strip()
                self.component_to_name_map[comp_id] = str(row.get(comp_name_col, '')).strip()
    
    def is_valid_component(self, component_id: str) -> bool:
        """Check if component ID exists in master table."""
        return str(component_id).strip() in self.valid_component_ids
    
    def get_valid_source(self, component_id: str) -> Optional[str]:
        """Get the valid parent source ID for a component."""
        return self.component_to_source_map.get(str(component_id).strip())
    
    def validate_component_source(
        self, 
        component_id: str, 
        extracted_source_id: str
    ) -> ValidationResult:
        """
        Validate that a component's extracted source matches master table.
        
        Args:
            component_id: The component identifier
            extracted_source_id: The source ID extracted from document
            
        Returns:
            ValidationResult with status and details
        """
        comp_id = str(component_id).strip()
        ext_source = str(extracted_source_id).strip()
        
        # Check if component exists in master table
        if not self.is_valid_component(comp_id):
            return ValidationResult(
                status=ValidationStatus.INVALID,
                confidence=0.0,
                reasoning=f"Component {comp_id} not found in master table",
                details={"error": "component_not_in_master"}
            )
        
        # Get valid source from master table
        valid_source = self.get_valid_source(comp_id)
        
        if not valid_source:
            return ValidationResult(
                status=ValidationStatus.REVIEW,
                confidence=50.0,
                reasoning=f"No valid source defined in master table for {comp_id}",
                details={"error": "no_valid_source_defined"}
            )
        
        # Compare extracted source with valid source
        if ext_source.lower() == valid_source.lower():
            return ValidationResult(
                status=ValidationStatus.VALID,
                confidence=100.0,
                reasoning=f"Source match confirmed: {ext_source}",
                details={"valid_source": valid_source, "match": True}
            )
        else:
            return ValidationResult(
                status=ValidationStatus.INVALID,
                confidence=0.0,
                reasoning=f"Source mismatch: extracted '{ext_source}' vs valid '{valid_source}'",
                details={
                    "valid_source": valid_source, 
                    "extracted_source": ext_source,
                    "match": False
                }
            )
    
    def validate_alignment_pair(
        self,
        component_a_id: str,
        component_b_id: str,
        source_a: str,
        source_b: str
    ) -> ValidationResult:
        """
        Validate an alignment pair against master table rules.
        
        Args:
            component_a_id: First component ID
            component_b_id: Second component ID
            source_a: Source document for component A
            source_b: Source document for component B
            
        Returns:
            ValidationResult with combined validation status
        """
        issues = []
        confidence = 100.0
        
        # Check both components exist in master table
        if not self.is_valid_component(component_a_id):
            issues.append(f"Component A ({component_a_id}) not in master table")
            confidence -= 50
        
        if not self.is_valid_component(component_b_id):
            issues.append(f"Component B ({component_b_id}) not in master table")
            confidence -= 50
        
        # Validate sources match master table
        valid_source_a = self.get_valid_source(component_a_id)
        valid_source_b = self.get_valid_source(component_b_id)
        
        if valid_source_a and source_a.lower() != valid_source_a.lower():
            issues.append(f"Component A source mismatch: {source_a} vs {valid_source_a}")
            confidence -= 25
        
        if valid_source_b and source_b.lower() != valid_source_b.lower():
            issues.append(f"Component B source mismatch: {source_b} vs {valid_source_b}")
            confidence -= 25
        
        confidence = max(0, confidence)
        
        if not issues:
            return ValidationResult(
                status=ValidationStatus.VALID,
                confidence=confidence,
                reasoning="All master table validations passed",
                details={"valid_source_a": valid_source_a, "valid_source_b": valid_source_b}
            )
        elif confidence >= 50:
            return ValidationResult(
                status=ValidationStatus.REVIEW,
                confidence=confidence,
                reasoning="; ".join(issues),
                details={"issues": issues}
            )
        else:
            return ValidationResult(
                status=ValidationStatus.INVALID,
                confidence=confidence,
                reasoning="; ".join(issues),
                details={"issues": issues}
            )
    
    def filter_valid_components(self, components_df: pd.DataFrame, id_column: str) -> pd.DataFrame:
        """
        Filter components DataFrame to only include those in master table.
        
        Args:
            components_df: DataFrame with components
            id_column: Name of the component ID column
            
        Returns:
            Filtered DataFrame
        """
        mask = components_df[id_column].astype(str).str.strip().isin(self.valid_component_ids)
        filtered = components_df[mask].copy()
        
        removed_count = len(components_df) - len(filtered)
        if removed_count > 0:
            logger.info(f"Filtered out {removed_count} components not in master table")
        
        return filtered


class SemanticRelationshipValidator:
    """
    Validates enabling relationships between component pairs using
    heuristic analysis and optional LLM validation.
    """
    
    # Keywords indicating enabling relationships
    ENABLING_KEYWORDS = [
        'enable', 'enables', 'enabling', 'support', 'supports', 'supporting',
        'provide', 'provides', 'providing', 'require', 'requires', 'required',
        'prerequisite', 'dependency', 'depends', 'dependent', 'foundation',
        'capability', 'resource', 'input', 'output', 'deliver', 'deliverable',
        'implement', 'implements', 'implementation', 'integrate', 'integration'
    ]
    
    # Domain-specific governance keywords
    GOVERNANCE_DOMAINS = {
        'security': ['security', 'authentication', 'authorization', 'access', 'compliance'],
        'data': ['data', 'database', 'analytics', 'reporting', 'warehouse'],
        'infrastructure': ['infrastructure', 'network', 'server', 'cloud', 'platform'],
        'application': ['application', 'software', 'system', 'module', 'interface'],
        'process': ['process', 'workflow', 'procedure', 'policy', 'governance']
    }
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the semantic relationship validator.
        
        Args:
            config: Configuration dictionary with thresholds and settings
        """
        self.config = config or {}
        self.enabling_threshold = self.config.get('enabling_relationship', 0.8)
        self.llm_client = None  # Will be initialized if LLM validation is enabled
        
        # Compile regex patterns
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for efficient matching."""
        self.enabling_pattern = re.compile(
            r'\b(' + '|'.join(self.ENABLING_KEYWORDS) + r')\b',
            re.IGNORECASE
        )
        
        self.domain_patterns = {
            domain: re.compile(r'\b(' + '|'.join(keywords) + r')\b', re.IGNORECASE)
            for domain, keywords in self.GOVERNANCE_DOMAINS.items()
        }
    
    def detect_domain(self, text: str) -> List[str]:
        """Detect governance domains mentioned in text."""
        domains = []
        for domain, pattern in self.domain_patterns.items():
            if pattern.search(text):
                domains.append(domain)
        return domains
    
    def calculate_enabling_score_heuristic(
        self,
        description_a: str,
        description_b: str,
        name_a: str = "",
        name_b: str = ""
    ) -> Tuple[float, str]:
        """
        Calculate enabling relationship score using heuristics.
        
        Args:
            description_a: Description of component A
            description_b: Description of component B
            name_a: Name of component A
            name_b: Name of component B
            
        Returns:
            Tuple of (score 0-1, reasoning string)
        """
        score = 0.0
        reasons = []
        
        # Combine name and description for analysis
        text_a = f"{name_a} {description_a}".lower()
        text_b = f"{name_b} {description_b}".lower()
        
        # Check for enabling keywords in both components
        keywords_a = set(self.enabling_pattern.findall(text_a))
        keywords_b = set(self.enabling_pattern.findall(text_b))
        
        if keywords_a and keywords_b:
            keyword_overlap = keywords_a.intersection(keywords_b)
            if keyword_overlap:
                score += 0.2
                reasons.append(f"Shared enabling keywords: {', '.join(keyword_overlap)}")
        
        # Check for complementary relationship patterns
        # A provides -> B requires
        if re.search(r'\b(provide|deliver|output)\b', text_a, re.I) and \
           re.search(r'\b(require|need|input|depend)\b', text_b, re.I):
            score += 0.3
            reasons.append("Complementary provide/require pattern detected")
        
        # Check domain relationships
        domains_a = set(self.detect_domain(text_a))
        domains_b = set(self.detect_domain(text_b))
        
        if domains_a and domains_b:
            if domains_a != domains_b:
                # Different domains - potential cross-domain enabling
                score += 0.2
                reasons.append(f"Cross-domain relationship: {domains_a} -> {domains_b}")
            else:
                # Same domain - need stronger evidence
                shared_domains = domains_a.intersection(domains_b)
                score += 0.1
                reasons.append(f"Same domain: {shared_domains}")
        
        # Check for specific enabling language patterns
        enabling_phrases = [
            (r'enables?\s+\w+', 0.15),
            (r'supports?\s+\w+', 0.1),
            (r'prerequisite\s+for', 0.2),
            (r'required\s+by', 0.15),
            (r'foundation\s+for', 0.15),
            (r'capability\s+to', 0.1)
        ]
        
        for pattern, weight in enabling_phrases:
            if re.search(pattern, text_a, re.I) or re.search(pattern, text_b, re.I):
                score += weight
                reasons.append(f"Enabling phrase pattern: {pattern}")
        
        # Normalize score to 0-1 range
        score = min(1.0, score)
        
        reasoning = "; ".join(reasons) if reasons else "No strong enabling indicators found"
        
        return score, reasoning
    
    def validate_relationship(
        self,
        component_a: Dict[str, Any],
        component_b: Dict[str, Any],
        use_llm: bool = False
    ) -> ValidationResult:
        """
        Validate the enabling relationship between two components.
        
        Args:
            component_a: Dict with component A data (id, name, description, source)
            component_b: Dict with component B data
            use_llm: Whether to use LLM for validation
            
        Returns:
            ValidationResult with enabling relationship assessment
        """
        desc_a = str(component_a.get('description', '')).strip()
        desc_b = str(component_b.get('description', '')).strip()
        name_a = str(component_a.get('name', '')).strip()
        name_b = str(component_b.get('name', '')).strip()
        source_a = str(component_a.get('source', '')).strip()
        source_b = str(component_b.get('source', '')).strip()
        
        # Validate inputs
        if len(desc_a) < 10 or len(desc_b) < 10:
            return ValidationResult(
                status=ValidationStatus.SKIPPED,
                confidence=0.0,
                reasoning="Insufficient description length for validation",
                details={"desc_a_len": len(desc_a), "desc_b_len": len(desc_b)}
            )
        
        # Same source check (should already be filtered, but double-check)
        if source_a and source_b and source_a.lower() == source_b.lower():
            return ValidationResult(
                status=ValidationStatus.INVALID,
                confidence=100.0,
                reasoning="Same source - cannot be enabling relationship",
                details={"source": source_a}
            )
        
        # Calculate heuristic score
        heuristic_score, heuristic_reasoning = self.calculate_enabling_score_heuristic(
            desc_a, desc_b, name_a, name_b
        )
        
        # Optional LLM validation
        llm_score = None
        llm_reasoning = None
        
        if use_llm and self.llm_client:
            llm_result = self._validate_with_llm(component_a, component_b)
            llm_score = llm_result.confidence / 100.0
            llm_reasoning = llm_result.reasoning
        
        # Combine scores
        if llm_score is not None:
            final_score = (heuristic_score * 0.4) + (llm_score * 0.6)
            combined_reasoning = f"Heuristic: {heuristic_reasoning}; LLM: {llm_reasoning}"
        else:
            final_score = heuristic_score
            combined_reasoning = heuristic_reasoning
        
        # Determine status based on threshold
        if final_score >= self.enabling_threshold:
            status = ValidationStatus.VALID
        elif final_score >= self.enabling_threshold * 0.75:
            status = ValidationStatus.REVIEW
        else:
            status = ValidationStatus.INVALID
        
        return ValidationResult(
            status=status,
            confidence=final_score * 100,
            reasoning=combined_reasoning,
            details={
                "heuristic_score": heuristic_score,
                "llm_score": llm_score,
                "final_score": final_score,
                "domains_a": self.detect_domain(f"{name_a} {desc_a}"),
                "domains_b": self.detect_domain(f"{name_b} {desc_b}")
            }
        )
    
    def _validate_with_llm(
        self,
        component_a: Dict[str, Any],
        component_b: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate relationship using LLM.
        
        Args:
            component_a: Component A data
            component_b: Component B data
            
        Returns:
            ValidationResult from LLM analysis
        """
        prompt = f"""
Component A: {component_a.get('name', 'Unknown')}
Description: {component_a.get('description', 'No description')}
Source: {component_a.get('source', 'Unknown')}

Component B: {component_b.get('name', 'Unknown')}
Description: {component_b.get('description', 'No description')}
Source: {component_b.get('source', 'Unknown')}

Validation Questions:
1. Would delivering Component A measurably progress Component B toward its delivery state?
2. Does Component A provide resources, capabilities, or prerequisites for Component B?
3. Are these components from different governance domains/systems?

Criteria for VALID alignment:
- Answer to Q1 is YES
- Components serve complementary purposes
- Clear enabling relationship exists

Response format: VALID or INVALID | Confidence (0-100) | Brief Reasoning
"""
        
        try:
            # Placeholder for actual LLM call
            # In production, this would call OpenAI, Azure, or local LLM
            logger.warning("LLM validation not implemented - using heuristic fallback")
            return ValidationResult(
                status=ValidationStatus.SKIPPED,
                confidence=0.0,
                reasoning="LLM validation not available",
                details={"prompt": prompt}
            )
        except Exception as e:
            logger.error(f"LLM validation error: {e}")
            return ValidationResult(
                status=ValidationStatus.ERROR,
                confidence=0.0,
                reasoning=f"LLM error: {str(e)}",
                details={"error": str(e)}
            )


class SameSourceFilter:
    """
    Filters out component pairs from the same source document.
    This is a hard rule - same-source pairs cannot be valid alignments.
    """
    
    def __init__(self):
        """Initialize the filter."""
        self.rejection_log: List[Dict[str, Any]] = []
    
    def filter_pairs(
        self,
        pairs_df: pd.DataFrame,
        source_a_col: str,
        source_b_col: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Filter out same-source pairs.
        
        Args:
            pairs_df: DataFrame with candidate pairs
            source_a_col: Column name for source A
            source_b_col: Column name for source B
            
        Returns:
            Tuple of (filtered_pairs, rejected_pairs)
        """
        # Normalize source values for comparison
        source_a_norm = pairs_df[source_a_col].astype(str).str.strip().str.lower()
        source_b_norm = pairs_df[source_b_col].astype(str).str.strip().str.lower()
        
        # Create mask for different sources
        different_source_mask = source_a_norm != source_b_norm
        
        filtered_df = pairs_df[different_source_mask].copy()
        rejected_df = pairs_df[~different_source_mask].copy()
        
        if len(rejected_df) > 0:
            rejected_df['Rejection_Reason'] = 'Same source document'
            self.rejection_log.extend(rejected_df.to_dict('records'))
            logger.info(f"Rejected {len(rejected_df)} same-source pairs")
        
        return filtered_df, rejected_df
    
    def get_rejection_report(self) -> pd.DataFrame:
        """Get DataFrame of all rejected pairs."""
        return pd.DataFrame(self.rejection_log)


def validate_components_batch(
    components_df: pd.DataFrame,
    master_validator: MasterTableValidator,
    id_column: str,
    source_column: str
) -> pd.DataFrame:
    """
    Batch validate components against master table.
    
    Args:
        components_df: DataFrame with components
        master_validator: MasterTableValidator instance
        id_column: Component ID column name
        source_column: Source column name
        
    Returns:
        DataFrame with validation results added
    """
    results = components_df.copy()
    
    validation_statuses = []
    valid_sources = []
    validation_notes = []
    
    for _, row in results.iterrows():
        comp_id = str(row[id_column]).strip()
        ext_source = str(row[source_column]).strip() if source_column in row else ""
        
        if ext_source:
            result = master_validator.validate_component_source(comp_id, ext_source)
        else:
            result = ValidationResult(
                status=ValidationStatus.REVIEW,
                confidence=50.0,
                reasoning="No extracted source to validate"
            )
        
        validation_statuses.append(result.status.value)
        valid_sources.append(master_validator.get_valid_source(comp_id) or "")
        validation_notes.append(result.reasoning)
    
    results['Validation_Status'] = validation_statuses
    results['Valid_Source_ID'] = valid_sources
    results['Validation_Notes'] = validation_notes
    
    return results
