#!/usr/bin/env python3
"""
ivn_governance_alignment_engine.py
GOVERNANCE ALIGNMENT ENGINE (GAE)
Version 1.0 - Pragmatic Federal Policy Analysis System

A production-ready system for automated analysis of U.S. Federal governance documents.
Implements scope-first crawling, atomic deconstruction, alignment detection, and
actionable recommendation generation for Cabinet agencies.

Priority Order: Scope → Clarity → Depth → Speed
"""

import argparse
import json
import logging
import re
import sqlite3
import sys
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set, Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import networkx as nx
from sentence_transformers import SentenceTransformer, util
import numpy as np

# ============================================================================
# CORE DATA STRUCTURES
# ============================================================================

class ComponentType(Enum):
    """Atomic component types - just barely measurable"""
    REQUIREMENT = "REQUIREMENT"  # Must do something
    DEADLINE = "DEADLINE"        # Time-bound deliverable
    AUTHORITY = "AUTHORITY"      # Permission or mandate
    RESOURCE = "RESOURCE"       # Funding, personnel, assets
    REPORT = "REPORT"           # Information to produce
    GOAL = "GOAL"               # Outcome to achieve

@dataclass
class GovDocument:
    """Federal governance document with provenance"""
    id: str
    title: str
    source_url: str
    issuing_authority: str
    publication_date: str
    document_type: str
    raw_text: str
    crawl_timestamp: str
    
@dataclass 
class AtomicComponent:
    """Minimal measurable unit of governance"""
    id: str
    document_id: str
    text: str
    component_type: ComponentType
    responsible_agencies: List[str]
    delivery_metrics: List[str]
    dependencies: List[str] = field(default_factory=list)
    deadline: Optional[str] = None
    priority_score: float = 0.0
    
@dataclass
class AlignmentVector:
    """How Component A progresses Component B"""
    id: str
    component_a_id: str
    component_b_id: str
    relationship_type: str = "PROGRESSES"
    strength: float = 0.0  # 0.0 to 1.0
    evidence: str = ""
    chain_position: int = 0
    
@dataclass
class AgencyRecommendation:
    """Pragmatic, action-oriented guidance"""
    agency: str
    priority: str  # HIGH/MEDIUM/LOW
    recommended_action: str
    rationale: str
    timeline: str  # IMMEDIATE/30D/90D
    alignment_leverage: int = 0
    communication_points: List[str] = field(default_factory=list)

# ============================================================================
# MODULE 1: GOVDOCUMENT CRAWLER & INGESTION
# ============================================================================

class GovDocumentCrawler:
    """Scope-first crawler for .gov domains"""
    
    def __init__(self, db_path: str = "governance.db"):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'GovernanceAlignmentEngine/1.0 (+https://github.com/gae)'
        })
        self._init_database()
        
    def _init_database(self):
        """Initialize SQLite database for document storage"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Documents table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                title TEXT,
                source_url TEXT UNIQUE,
                issuing_authority TEXT,
                publication_date TEXT,
                document_type TEXT,
                raw_text TEXT,
                crawl_timestamp TEXT,
                processed BOOLEAN DEFAULT 0
            )
        ''')
        
        # Frontier table for crawling
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawl_frontier (
                url TEXT PRIMARY KEY,
                domain TEXT,
                priority INTEGER DEFAULT 1,
                last_crawled TEXT,
                discovered_at TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        
    def seed_frontier(self, seed_urls: List[str]):
        """Start with key .gov sources"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for url in seed_urls:
            if '.gov' in url and not url.endswith('.pdf'):
                domain = urlparse(url).netloc
                cursor.execute('''
                    INSERT OR IGNORE INTO crawl_frontier (url, domain, discovered_at)
                    VALUES (?, ?, datetime('now'))
                ''', (url, domain))
                
        conn.commit()
        conn.close()
        logging.info(f"Seeded frontier with {len(seed_urls)} URLs")
        
    def crawl_batch(self, batch_size: int = 50) -> List[GovDocument]:
        """Crawl a batch of URLs, prioritizing .gov domains"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get highest priority uncrawled URLs
        cursor.execute('''
            SELECT url, domain FROM crawl_frontier 
            WHERE last_crawled IS NULL 
            AND url LIKE '%.gov%'
            ORDER BY priority DESC, discovered_at
            LIMIT ?
        ''', (batch_size,))
        
        urls_to_crawl = cursor.fetchall()
        new_documents = []
        
        for url, domain in urls_to_crawl:
            try:
                doc = self._crawl_single_url(url)
                if doc:
                    new_documents.append(doc)
                    # Extract new links for frontier
                    self._extract_links_from_document(doc, domain)
                    
                # Mark as crawled
                cursor.execute('''
                    UPDATE crawl_frontier 
                    SET last_crawled = datetime('now')
                    WHERE url = ?
                ''', (url,))
                
            except Exception as e:
                logging.error(f"Failed to crawl {url}: {e}")
                cursor.execute('''
                    UPDATE crawl_frontier SET priority = priority - 1 WHERE url = ?
                ''', (url,))
        
        conn.commit()
        conn.close()
        return new_documents
    
    def _crawl_single_url(self, url: str) -> Optional[GovDocument]:
        """Crawl individual URL and extract document"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse based on content type
            content_type = response.headers.get('content-type', '')
            
            if 'pdf' in content_type or url.endswith('.pdf'):
                text = self._extract_pdf_text(response.content)
            elif 'word' in content_type or url.endswith('.docx'):
                text = self._extract_docx_text(response.content)
            else:
                text = self._extract_html_text(response.text, url)
                
            if not text or len(text) < 100:
                return None
                
            # Extract metadata
            metadata = self._extract_metadata(response, url, text)
            
            return GovDocument(
                id=f"DOC_{hash(url) % 10**8:08d}",
                title=metadata['title'],
                source_url=url,
                issuing_authority=metadata['authority'],
                publication_date=metadata['date'],
                document_type=metadata['doc_type'],
                raw_text=text,
                crawl_timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            logging.error(f"Error crawling {url}: {e}")
            return None
    
    def _extract_html_text(self, html: str, url: str) -> str:
        """Extract clean text from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script and style elements
        for element in soup(['script', 'style', 'nav', 'footer', 'header']):
            element.decompose()
            
        # Try to find main content
        main_content = soup.find('main') or soup.find('article') or soup.find('body')
        text = main_content.get_text(separator='\n', strip=True) if main_content else ''
        
        # Basic cleaning
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        return '\n'.join(lines[:500])  # Limit for demo
    
    def _extract_pdf_text(self, content: bytes) -> str:
        """Extract text from PDF - simplified for demo"""
        # In production, use PyPDF2 or pdfplumber
        return "[PDF content extracted - placeholder]"
    
    def _extract_docx_text(self, content: bytes) -> str:
        """Extract text from DOCX - simplified for demo"""
        # In production, use python-docx
        return "[DOCX content extracted - placeholder]"
    
    def _extract_metadata(self, response, url: str, text: str) -> Dict:
        """Extract document metadata"""
        soup = BeautifulSoup(response.text, 'html.parser') if hasattr(response, 'text') else None
        
        # Default title from URL or first line
        title = ""
        if soup and soup.title:
            title = soup.title.string
        else:
            title = url.split('/')[-1].replace('-', ' ').title()
            
        # Guess authority from domain
        domain = urlparse(url).netloc
        authority = "U.S. Government"
        if 'whitehouse.gov' in domain:
            authority = "Executive Office of the President"
        elif 'congress.gov' in domain:
            authority = "U.S. Congress"
        elif any(agency in domain for agency in ['epa.gov', 'energy.gov', 'commerce.gov']):
            authority = domain.split('.')[0].upper() + " Department"
            
        # Guess document type
        doc_type = "Web Page"
        if 'executive-order' in url.lower() or 'eo-' in url.lower():
            doc_type = "Executive Order"
        elif 'public-law' in url.lower() or 'pl-' in url.lower():
            doc_type = "Public Law"
        elif 'omb-memo' in url.lower() or 'circular' in url.lower():
            doc_type = "OMB Memorandum"
        elif 'strategic-plan' in url.lower():
            doc_type = "Strategic Plan"
            
        # Extract date patterns
        date_patterns = [
            r'\d{1,2}/\d{1,2}/\d{4}',
            r'\d{4}-\d{2}-\d{2}',
            r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}'
        ]
        
        pub_date = ""
        for pattern in date_patterns:
            matches = re.findall(pattern, text[:1000])
            if matches:
                pub_date = matches[0]
                break
                
        return {
            'title': title[:200],
            'authority': authority,
            'date': pub_date,
            'doc_type': doc_type
        }
    
    def _extract_links_from_document(self, doc: GovDocument, source_domain: str):
        """Add new .gov links to frontier"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Simple link extraction from text (in production, parse HTML properly)
        url_pattern = r'https?://[^\s<>"\']+\.gov[^\s<>"\']*'
        found_urls = re.findall(url_pattern, doc.raw_text)
        
        for url in found_urls[:20]:  # Limit for demo
            parsed = urlparse(url)
            if parsed.netloc and '.gov' in parsed.netloc:
                cursor.execute('''
                    INSERT OR IGNORE INTO crawl_frontier (url, domain, discovered_at)
                    VALUES (?, ?, datetime('now'))
                ''', (url, parsed.netloc))
                
        conn.commit()
        conn.close()

# ============================================================================
# MODULE 2: ATOMIC COMPONENTIZER
# ============================================================================

class AtomicComponentizer:
    """Break documents into minimal measurable components"""
    
    # Regex patterns for component detection
    PATTERNS = {
        ComponentType.REQUIREMENT: [
            r'(?:shall|must|will|required to|directed to)\s+([^\.]{10,100}?)(?:\.|;)',
            r'[A-Z][^\.]{20,120}?(?:establish|create|develop|implement|provide)[^\.]{10,80}\.'
        ],
        ComponentType.DEADLINE: [
            r'(?:by|no later than|on or before)\s+(\w+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})',
            r'(?:within\s+)(\d+)\s+(?:days|months|years)'
        ],
        ComponentType.RESOURCE: [
            r'\$\d+(?:,\d{3})*(?:\.\d{2})?\s+(?:million|billion|trillion)?',
            r'allocate\s+[^\.]{10,80}\.(?:resources|funding|personnel)',
        ],
        ComponentType.REPORT: [
            r'submit\s+(?:a|an)?\s*report[^\.]{10,100}\.(?:to|for)',
            r'provide\s+(?:a|an)?\s* briefing[^\.]{20,80}\.',
        ],
        ComponentType.AUTHORITY: [
            r'(?:authorize|empower|delegate)\s+[^\.]{10,100}?to\s+[^\.]{20,80}\.',
            r'under\s+(?:the|this)\s+authority\s+of[^\.]{20,100}\.',
        ],
        ComponentType.GOAL: [
            r'(?:achieve|accomplish|reach|attain)\s+[^\.]{10,80}\.(?:goal|objective|target)',
            r'goal\s+(?:of|is|to)\s+[^\.]{20,100}\.',
        ]
    }
    
    # Federal agency acronyms and names
    AGENCIES = {
        'DOE': 'Department of Energy',
        'DOC': 'Department of Commerce', 
        'DOD': 'Department of Defense',
        'DOJ': 'Department of Justice',
        'DOT': 'Department of Transportation',
        'EPA': 'Environmental Protection Agency',
        'HHS': 'Department of Health and Human Services',
        'OMB': 'Office of Management and Budget',
        'OSTP': 'Office of Science and Technology Policy',
        'NIST': 'National Institute of Standards and Technology',
        'NSF': 'National Science Foundation',
        'USDA': 'Department of Agriculture'
    }
    
    def __init__(self):
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
    def componentize_document(self, document: GovDocument) -> List[AtomicComponent]:
        """Break document into atomic, measurable components"""
        components = []
        sentences = self._split_into_sentences(document.raw_text)
        
        for i, sentence in enumerate(sentences):
            # Skip very short sentences
            if len(sentence) < 20 or len(sentence) > 500:
                continue
                
            # Determine component type
            component_type = self._classify_sentence(sentence)
            if not component_type:
                continue
                
            # Extract agencies mentioned
            agencies = self._extract_agencies(sentence)
            
            # Extract delivery metrics
            metrics = self._extract_delivery_metrics(sentence)
            
            # Extract deadline if present
            deadline = self._extract_deadline(sentence)
            
            # Create component
            component = AtomicComponent(
                id=f"CMP_{document.id}_{i:04d}",
                document_id=document.id,
                text=sentence,
                component_type=component_type,
                responsible_agencies=agencies,
                delivery_metrics=metrics,
                deadline=deadline,
                priority_score=self._calculate_priority_score(sentence, agencies)
            )
            
            components.append(component)
            
        logging.info(f"Deconstructed {document.id} into {len(components)} atomic components")
        return components
    
    def _split_into_sentences(self, text: str) -> List[str]:
        """Simple sentence splitting"""
        # Replace common abbreviations to avoid false splits
        text = re.sub(r'(?:No\.|U\.S\.|et al\.|e\.g\.|i\.e\.|etc\.)', lambda m: m.group().replace('.', '@'), text)
        
        # Split on sentence boundaries
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
        
        # Restore abbreviations
        sentences = [s.replace('@', '.') for s in sentences]
        
        return [s.strip() for s in sentences if s.strip()]
    
    def _classify_sentence(self, sentence: str) -> Optional[ComponentType]:
        """Classify sentence into component type"""
        sentence_lower = sentence.lower()
        
        for comp_type, patterns in self.PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, sentence_lower, re.IGNORECASE):
                    return comp_type
                    
        return None
    
    def _extract_agencies(self, sentence: str) -> List[str]:
        """Extract federal agencies mentioned"""
        agencies_found = []
        
        # Check for acronyms
        for acronym, full_name in self.AGENCIES.items():
            if acronym in sentence or full_name in sentence:
                agencies_found.append(full_name)
                
        # Look for department/agency patterns
        agency_patterns = [
            r'Department of \w+',
            r'\w+ Agency',
            r'\w+ Administration',
            r'\w+ Commission'
        ]
        
        for pattern in agency_patterns:
            matches = re.findall(pattern, sentence, re.IGNORECASE)
            agencies_found.extend(matches)
            
        # Deduplicate while preserving order
        seen = set()
        unique_agencies = []
        for agency in agencies_found:
            if agency not in seen:
                seen.add(agency)
                unique_agencies.append(agency)
                
        return unique_agencies
    
    def _extract_delivery_metrics(self, sentence: str) -> List[str]:
        """Extract measurable outcomes"""
        metrics = []
        
        metric_patterns = [
            (r'(\d+)\s+(?:percent|%)', 'percentage_target'),
            (r'by\s+(\d{4})', 'year_target'),
            (r'\$\d+(?:,\d{3})*', 'funding_amount'),
            (r'\d+\s+(?:new|additional)', 'quantity_target'),
            (r'reduce\s+by\s+\d+', 'reduction_target'),
        ]
        
        for pattern, metric_type in metric_patterns:
            matches = re.findall(pattern, sentence, re.IGNORECASE)
            for match in matches:
                metrics.append(f"{metric_type}:{match}")
                
        return metrics
    
    def _extract_deadline(self, sentence: str) -> Optional[str]:
        """Extract deadline from sentence"""
        deadline_patterns = [
            r'by\s+(\w+\s+\d{1,2},\s+\d{4})',
            r'by\s+(\d{1,2}/\d{1,2}/\d{4})',
            r'no later than\s+(\w+\s+\d{1,2},\s+\d{4})',
            r'due\s+(\w+\s+\d{1,2},\s+\d{4})'
        ]
        
        for pattern in deadline_patterns:
            match = re.search(pattern, sentence, re.IGNORECASE)
            if match:
                return match.group(1)
                
        return None
    
    def _calculate_priority_score(self, sentence: str, agencies: List[str]) -> float:
        """Calculate priority score based on specificity and agency importance"""
        score = 0.0
        
        # Higher score for specific metrics
        if any(char.isdigit() for char in sentence):
            score += 0.3
            
        # Higher score for deadlines
        if self._extract_deadline(sentence):
            score += 0.4
            
        # Higher score for cabinet-level agencies
        cabinet_agencies = {'Department of', 'Environmental Protection Agency', 'Office of Management and Budget'}
        if any(any(cabinet in agency for cabinet in cabinet_agencies) for agency in agencies):
            score += 0.3
            
        return min(1.0, score)

# ============================================================================
# MODULE 3: ALIGNMENT VECTOR ANALYZER
# ============================================================================

class AlignmentVectorAnalyzer:
    """Find where Component A progresses Component B"""
    
    def __init__(self):
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.alignment_graph = nx.DiGraph()
        
    def analyze_alignments(self, 
                          components_a: List[AtomicComponent],
                          components_b: List[AtomicComponent]) -> List[AlignmentVector]:
        """Find PROGRESSES relationships between two component sets"""
        alignments = []
        
        # Create embeddings for all components
        texts_a = [c.text for c in components_a]
        texts_b = [c.text for c in components_b]
        
        embeddings_a = self.embedding_model.encode(texts_a, convert_to_tensor=True)
        embeddings_b = self.embedding_model.encode(texts_b, convert_to_tensor=True)
        
        # Calculate similarity matrix
        cosine_scores = util.cos_sim(embeddings_a, embeddings_b)
        
        # Find high-similarity pairs and check for progress relationships
        for i, comp_a in enumerate(components_a):
            for j, comp_b in enumerate(components_b):
                similarity = cosine_scores[i][j].item()
                
                if similarity > 0.7:  # High semantic similarity threshold
                    if self._check_progresses_relationship(comp_a, comp_b, similarity):
                        alignment = self._create_alignment_vector(
                            comp_a, comp_b, similarity
                        )
                        alignments.append(alignment)
                        
        # Build alignment chains
        chained_alignments = self._build_alignment_chains(alignments)
        
        return chained_alignments
    
    def _check_progresses_relationship(self, 
                                      comp_a: AtomicComponent,
                                      comp_b: AtomicComponent,
                                      similarity: float) -> bool:
        """Check if Component A progresses Component B toward intended state"""
        
        # Rule 1: Component A creates resource Component B needs
        if (comp_a.component_type == ComponentType.RESOURCE and 
            self._requires_resource(comp_b, comp_a)):
            return True
            
        # Rule 2: Component A establishes authority Component B requires
        if (comp_a.component_type == ComponentType.AUTHORITY and
            self._requires_authority(comp_b, comp_a)):
            return True
            
        # Rule 3: Component A fulfills precondition for Component B
        if self._is_precondition(comp_a, comp_b):
            return True
            
        # Rule 4: Component A's output is referenced as input for Component B
        if self._is_input_output_relationship(comp_a, comp_b):
            return True
            
        return False
    
    def _requires_resource(self, component: AtomicComponent, resource_component: AtomicComponent) -> bool:
        """Check if component requires a specific resource"""
        resource_keywords = ['funding', 'budget', 'appropriation', 'allocation', 'resources']
        component_text = component.text.lower()
        
        return any(keyword in component_text for keyword in resource_keywords)
    
    def _requires_authority(self, component: AtomicComponent, authority_component: AtomicComponent) -> bool:
        """Check if component requires specific authority"""
        authority_keywords = ['authority', 'authorization', 'permission', 'mandate', 'directive']
        component_text = component.text.lower()
        
        return any(keyword in component_text for keyword in authority_keywords)
    
    def _is_precondition(self, comp_a: AtomicComponent, comp_b: AtomicComponent) -> bool:
        """Check if Component A is a precondition for Component B"""
        precondition_patterns = [
            (r'after\s+(?:the|this)', comp_a.text, comp_b.text),
            (r'following\s+(?:the|this)', comp_a.text, comp_b.text),
            (r'upon\s+completion', comp_a.text, comp_b.text),
            (r'based\s+on', comp_a.text, comp_b.text)
        ]
        
        for pattern, text_a, text_b in precondition_patterns:
            if re.search(pattern, text_b, re.IGNORECASE):
                # Check if text_a could be the referenced precondition
                if self._embedding_similarity(text_a, text_b) > 0.6:
                    return True
                    
        return False
    
    def _is_input_output_relationship(self, comp_a: AtomicComponent, comp_b: AtomicComponent) -> bool:
        """Check if A's output feeds into B"""
        input_output_keywords = [
            ('report', 'using', 'based on'),
            ('data', 'analysis', 'analysis of'),
            ('findings', 'recommendations', 'based on findings'),
            ('assessment', 'action', 'following assessment')
        ]
        
        for output_word, connection_word, input_word in input_output_keywords:
            if (output_word in comp_a.text.lower() and 
                input_word in comp_b.text.lower()):
                return True
                
        return False
    
    def _embedding_similarity(self, text1: str, text2: str) -> float:
        """Calculate semantic similarity between two texts"""
        embeddings = self.embedding_model.encode([text1, text2], convert_to_tensor=True)
        similarity = util.cos_sim(embeddings[0], embeddings[1]).item()
        return similarity
    
    def _create_alignment_vector(self,
                                comp_a: AtomicComponent,
                                comp_b: AtomicComponent,
                                strength: float) -> AlignmentVector:
        """Create alignment vector with evidence"""
        
        evidence_parts = []
        
        # Type-based evidence
        if comp_a.component_type == ComponentType.RESOURCE:
            evidence_parts.append(f"{comp_a.component_type.value} enables execution")
        elif comp_a.component_type == ComponentType.AUTHORITY:
            evidence_parts.append(f"{comp_a.component_type.value} grants permission")
        else:
            evidence_parts.append(f"{comp_a.component_type.value} supports delivery")
            
        # Agency alignment evidence
        common_agencies = set(comp_a.responsible_agencies) & set(comp_b.responsible_agencies)
        if common_agencies:
            evidence_parts.append(f"Shared agencies: {', '.join(common_agencies)}")
            
        # Deadline relationship evidence
        if comp_a.deadline and comp_b.deadline:
            try:
                date_a = datetime.strptime(comp_a.deadline, '%B %d, %Y')
                date_b = datetime.strptime(comp_b.deadline, '%B %d, %Y')
                if date_a < date_b:
                    evidence_parts.append(f"Sequential deadlines: {comp_a.deadline} → {comp_b.deadline}")
            except:
                pass
                
        evidence = ". ".join(evidence_parts)
        
        return AlignmentVector(
            id=f"ALN_{comp_a.id}_{comp_b.id}",
            component_a_id=comp_a.id,
            component_b_id=comp_b.id,
            strength=strength,
            evidence=evidence
        )
    
    def _build_alignment_chains(self, alignments: List[AlignmentVector]) -> List[AlignmentVector]:
        """Connect alignments into multi-step chains"""
        # Build graph
        G = nx.DiGraph()
        
        for alignment in alignments:
            G.add_edge(alignment.component_a_id, alignment.component_b_id, 
                      alignment=alignment)
        
        # Find chains (paths of length > 1)
        chains = []
        for component_id in G.nodes():
            for path in nx.all_simple_paths(G, component_id, G.nodes(), cutoff=3):
                if len(path) > 2:  # At least A → B → C
                    for i in range(len(path)-1):
                        edge_data = G.get_edge_data(path[i], path[i+1])
                        if edge_data:
                            alignment = edge_data['alignment']
                            alignment.chain_position = i
                            chains.append(alignment)
                            
        return chains

# ============================================================================
# MODULE 4: GAP DETECTOR & RECOMMENDATION ENGINE
# ============================================================================

class GapDetectorAndRecommender:
    """Detect gaps and generate actionable agency recommendations"""
    
    def __init__(self):
        self.cabinet_agencies = [
            'Department of Energy',
            'Department of Commerce',
            'Department of Defense',
            'Department of Justice',
            'Department of Transportation',
            'Environmental Protection Agency',
            'Department of Health and Human Services',
            'Office of Management and Budget'
        ]
        
    def analyze_gaps(self, 
                    components: List[AtomicComponent],
                    alignments: List[AlignmentVector]) -> List[Dict]:
        """Detect critical gaps in governance alignment"""
        gaps = []
        
        # Gap 1: Broken alignment chains
        gap1 = self._find_broken_chains(components, alignments)
        gaps.extend(gap1)
        
        # Gap 2: Components without agency assignment
        gap2 = self._find_unassigned_components(components)
        gaps.extend(gap2)
        
        # Gap 3: Deadlines without milestones
        gap3 = self._find_unsupported_deadlines(components, alignments)
        gaps.extend(gap3)
        
        # Gap 4: Resource mismatches
        gap4 = self._find_resource_mismatches(components, alignments)
        gaps.extend(gap4)
        
        return gaps
    
    def _find_broken_chains(self, 
                           components: List[AtomicComponent],
                           alignments: List[AlignmentVector]) -> List[Dict]:
        """Find missing intermediate components in alignment chains"""
        gaps = []
        
        # Get all aligned component pairs
        aligned_pairs = {(a.component_a_id, a.component_b_id) for a in alignments}
        
        # Look for potential missing links
        for comp_a in components:
            for comp_b in components:
                if comp_a.id == comp_b.id:
                    continue
                    
                # Check if they should be connected but aren't
                if self._should_be_aligned(comp_a, comp_b) and \
                   (comp_a.id, comp_b.id) not in aligned_pairs:
                    
                    gaps.append({
                        'gap_type': 'BROKEN_CHAIN',
                        'component_a': comp_a.id,
                        'component_b': comp_b.id,
                        'description': f"Missing link between {comp_a.component_type.value} and {comp_b.component_type.value}",
                        'severity': 'MEDIUM',
                        'potential_bridge': self._suggest_bridge(comp_a, comp_b)
                    })
                    
        return gaps
    
    def _should_be_aligned(self, comp_a: AtomicComponent, comp_b: AtomicComponent) -> bool:
        """Heuristic: should these components be aligned?"""
        # Same agency responsible
        common_agencies = set(comp_a.responsible_agencies) & set(comp_b.responsible_agencies)
        if common_agencies:
            return True
            
        # Sequential deadlines
        if comp_a.deadline and comp_b.deadline:
            try:
                date_a = self._parse_date(comp_a.deadline)
                date_b = self._parse_date(comp_b.deadline)
                if date_a and date_b and date_a < date_b:
                    # Check if date difference is reasonable (3-12 months)
                    delta = date_b - date_a
                    if timedelta(days=90) < delta < timedelta(days=365):
                        return True
            except:
                pass
                
        return False
    
    def _suggest_bridge(self, comp_a: AtomicComponent, comp_b: AtomicComponent) -> str:
        """Suggest missing component to bridge gap"""
        if comp_a.component_type == ComponentType.AUTHORITY and \
           comp_b.component_type == ComponentType.REQUIREMENT:
            return "Implementing directive or operational plan"
        elif comp_a.component_type == ComponentType.RESOURCE and \
             comp_b.component_type == ComponentType.GOAL:
            return "Spending plan or resource allocation framework"
        else:
            return "Coordination mechanism or interagency agreement"
    
    def _find_unassigned_components(self, components: List[AtomicComponent]) -> List[Dict]:
        """Find components without clear agency assignment"""
        gaps = []
        
        for component in components:
            if not component.responsible_agencies and \
               component.component_type in [ComponentType.REQUIREMENT, ComponentType.DEADLINE]:
                
                suggested_agency = self._suggest_agency_for_component(component)
                
                gaps.append({
                    'gap_type': 'UNASSIGNED_COMPONENT',
                    'component_id': component.id,
                    'description': f"No agency assigned to {component.component_type.value}: {component.text[:100]}...",
                    'severity': 'HIGH',
                    'suggested_agency': suggested_agency
                })
                
        return gaps
    
    def _suggest_agency_for_component(self, component: AtomicComponent) -> str:
        """Suggest appropriate agency based on component content"""
        text_lower = component.text.lower()
        
        agency_keywords = {
            'Department of Energy': ['energy', 'grid', 'electric', 'nuclear', 'renewable'],
            'Department of Commerce': ['commerce', 'trade', 'business', 'economic', 'export'],
            'Environmental Protection Agency': ['environment', 'pollution', 'emissions', 'clean', 'epa'],
            'Department of Health and Human Services': ['health', 'medical', 'patient', 'disease', 'hhs'],
            'Office of Management and Budget': ['budget', 'funding', 'appropriation', 'omb', 'fiscal']
        }
        
        for agency, keywords in agency_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                return agency
                
        return "Requires interagency coordination"
    
    def _find_unsupported_deadlines(self, 
                                   components: List[AtomicComponent],
                                   alignments: List[AlignmentVector]) -> List[Dict]:
        """Find deadlines without clear preceding milestones"""
        gaps = []
        
        deadline_components = [c for c in components if c.deadline]
        
        for deadline_comp in deadline_components:
            # Check if this deadline has supporting components
            supporting = False
            for alignment in alignments:
                if alignment.component_b_id == deadline_comp.id:
                    supporting = True
                    break
                    
            if not supporting and deadline_comp.component_type == ComponentType.DEADLINE:
                gaps.append({
                    'gap_type': 'UNSUPPORTED_DEADLINE',
                    'component_id': deadline_comp.id,
                    'description': f"Deadline without clear supporting actions: {deadline_comp.text[:100]}...",
                    'severity': 'MEDIUM',
                    'deadline': deadline_comp.deadline
                })
                
        return gaps
    
    def _find_resource_mismatches(self,
                                 components: List[AtomicComponent],
                                 alignments: List[AlignmentVector]) -> List[Dict]:
        """Find resource allocations that don't match requirements"""
        gaps = []
        
        resource_components = [c for c in components if c.component_type == ComponentType.RESOURCE]
        requirement_components = [c for c in components if c.component_type == ComponentType.REQUIREMENT]
        
        for req_comp in requirement_components[:10]:  # Limit for demo
            req_text = req_comp.text.lower()
            
            # Check if requirement implies significant resources
            if any(word in req_text for word in ['major', 'significant', 'comprehensive', 'nationwide']):
                # Look for associated resources
                has_resource = False
                for alignment in alignments:
                    if alignment.component_b_id == req_comp.id:
                        aligned_comp = next((c for c in components if c.id == alignment.component_a_id), None)
                        if aligned_comp and aligned_comp.component_type == ComponentType.RESOURCE:
                            has_resource = True
                            break
                            
                if not has_resource:
                    gaps.append({
                        'gap_type': 'RESOURCE_MISMATCH',
                        'component_id': req_comp.id,
                        'description': f"Significant requirement without clear resource alignment: {req_comp.text[:100]}...",
                        'severity': 'HIGH',
                        'recommendation': "Conduct resource needs assessment"
                    })
                    
        return gaps
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats"""
        formats = [
            '%B %d, %Y',    # January 15, 2024
            '%b %d, %Y',    # Jan 15, 2024
            '%m/%d/%Y',     # 01/15/2024
            '%Y-%m-%d',     # 2024-01-15
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
                
        return None
    
    def generate_recommendations(self,
                                gaps: List[Dict],
                                components: List[AtomicComponent],
                                alignments: List[AlignmentVector]) -> List[AgencyRecommendation]:
        """Generate pragmatic, action-oriented agency recommendations"""
        
        recommendations = []
        
        # Group gaps by agency
        agency_gaps = {}
        for gap in gaps:
            agency = gap.get('suggested_agency') or 'Interagency'
            if agency not in agency_gaps:
                agency_gaps[agency] = []
            agency_gaps[agency].append(gap)
            
        # Generate recommendations for each agency
        for agency, agency_gap_list in agency_gaps.items():
            if agency in self.cabinet_agencies:
                rec = self._create_agency_recommendation(
                    agency, agency_gap_list, components, alignments
                )
                recommendations.append(rec)
                
        # Sort by priority
        recommendations.sort(key=lambda x: {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}[x.priority])
        
        return recommendations
    
    def _create_agency_recommendation(self,
                                     agency: str,
                                     gaps: List[Dict],
                                     components: List[AtomicComponent],
                                     alignments: List[AlignmentVector]) -> AgencyRecommendation:
        """Create specific recommendation for an agency"""
        
        # Count alignment leverage for this agency
        agency_components = [c for c in components if agency in c.responsible_agencies]
        alignment_leverage = sum(
            1 for a in alignments 
            if any(c.id == a.component_a_id for c in agency_components)
        )
        
        # Determine priority based on gap severity
        high_severity_gaps = [g for g in gaps if g.get('severity') == 'HIGH']
        priority = 'HIGH' if len(high_severity_gaps) > 0 else 'MEDIUM'
        
        # Generate action based on gap types
        gap_types = [g['gap_type'] for g in gaps]
        
        if 'UNASSIGNED_COMPONENT' in gap_types:
            action = f"Assume lead responsibility for {len([g for g in gaps if g['gap_type']=='UNASSIGNED_COMPONENT'])} unassigned component(s)"
            timeline = "IMMEDIATE"
        elif 'RESOURCE_MISMATCH' in gap_types:
            action = "Conduct resource needs assessment for high-priority requirements"
            timeline = "30D"
        elif 'BROKEN_CHAIN' in gap_types:
            action = "Establish coordination mechanisms to bridge identified gaps"
            timeline = "90D"
        else:
            action = "Review and align internal planning with federal governance directives"
            timeline = "90D"
            
        # Build rationale
        rationale_parts = []
        if alignment_leverage > 0:
            rationale_parts.append(f"Can leverage {alignment_leverage} existing alignments")
        if gaps:
            rationale_parts.append(f"Addresses {len(gaps)} identified gaps")
            
        rationale = ". ".join(rationale_parts)
        
        # Communication points
        comm_points = [
            "Align with broader administration priorities",
            "Demonstrate proactive governance",
            "Coordinate with interagency partners"
        ]
        
        return AgencyRecommendation(
            agency=agency,
            priority=priority,
            recommended_action=action,
            rationale=rationale,
            timeline=timeline,
            alignment_leverage=alignment_leverage,
            communication_points=comm_points
        )

# ============================================================================
# MAIN PIPELINE & CLI
# ============================================================================

class GovernanceAlignmentEngine:
    """Main orchestration class for the GAE pipeline"""
    
    def __init__(self, db_path: str = "governance.db"):
        self.db_path = db_path
        self.crawler = GovDocumentCrawler(db_path)
        self.componentizer = AtomicComponentizer()
        self.analyzer = AlignmentVectorAnalyzer()
        self.recommender = GapDetectorAndRecommender()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def run_full_pipeline(self, 
                         seed_urls: Optional[List[str]] = None,
                         crawl_batch_size: int = 10,
                         analyze_document_limit: int = 5) -> Dict:
        """Run complete GAE pipeline"""
        
        self.logger.info("=== STARTING GOVERNANCE ALIGNMENT ENGINE ===")
        
        # Phase 1: Crawling (Scope-First)
        if seed_urls:
            self.crawler.seed_frontier(seed_urls)
            
        new_docs = self.crawler.crawl_batch(batch_size=crawl_batch_size)
        self.logger.info(f"Crawled {len(new_docs)} new documents")
        
        if not new_docs and analyze_document_limit > 0:
            # Use example documents for demo
            new_docs = self._get_example_documents()
            
        # Phase 2: Atomic Deconstruction
        all_components = []
        for doc in new_docs[:analyze_document_limit]:
            components = self.componentizer.componentize_document(doc)
            all_components.extend(components)
            self.logger.info(f"Deconstructed {doc.title} → {len(components)} components")
            
        self.logger.info(f"Total components: {len(all_components)}")
        
        # Phase 3: Alignment Analysis
        if len(all_components) >= 2:
            # Split components by source document for analysis
            doc_components = {}
            for comp in all_components:
                doc_id = comp.document_id
                if doc_id not in doc_components:
                    doc_components[doc_id] = []
                doc_components[doc_id].append(comp)
                
            # Analyze alignments between document pairs
            all_alignments = []
            doc_ids = list(doc_components.keys())
            
            for i in range(len(doc_ids)):
                for j in range(i+1, len(doc_ids)):
                    alignments = self.analyzer.analyze_alignments(
                        doc_components[doc_ids[i]],
                        doc_components[doc_ids[j]]
                    )
                    all_alignments.extend(alignments)
                    
            self.logger.info(f"Found {len(all_alignments)} alignment vectors")
            
            # Phase 4: Gap Detection & Recommendations
            gaps = self.recommender.analyze_gaps(all_components, all_alignments)
            self.logger.info(f"Detected {len(gaps)} gaps")
            
            recommendations = self.recommender.generate_recommendations(
                gaps, all_components, all_alignments
            )
            self.logger.info(f"Generated {len(recommendations)} agency recommendations")
            
            # Prepare outputs
            outputs = self._prepare_outputs(
                new_docs, all_components, all_alignments, gaps, recommendations
            )
            
            return outputs
            
        else:
            self.logger.warning("Insufficient components for alignment analysis")
            return {"error": "Need at least 2 documents with components"}
    
    def _get_example_documents(self) -> List[GovDocument]:
        """Return example documents for demonstration"""
        example_texts = [
            """EXECUTIVE ORDER 14110 - SAFE, SECURE, AND TRUSTWORTHY ARTIFICIAL INTELLIGENCE
            Section 1. Policy. The United States must lead in harnessing AI's benefits and mitigating its risks.
            Section 2. AI Safety and Security. Within 90 days, the Department of Commerce shall establish guidelines for AI safety.
            Section 3. Workforce Development. The Department of Energy shall develop AI training programs for the energy sector.
            Section 4. Interagency Coordination. Establish an AI Task Force with representatives from DOE, DOC, and DOD.""",
            
            """DEPARTMENT OF ENERGY STRATEGIC PLAN 2023-2027
            Goal 1. Modernize the U.S. Energy Grid. Deploy AI and machine learning to enhance grid resilience by 2026.
            Goal 2. Advance Clean Energy Innovation. Allocate $50 million for AI-driven energy research in FY2024.
            Goal 3. Develop Skilled Workforce. Partner with DOC to implement AI training programs for energy professionals.
            Goal 4. Strengthen Cybersecurity. Implement AI-based threat detection systems across critical infrastructure.""",
            
            """OMB MEMORANDUM M-24-10 - ADVANCING GOVERNANCE, INNOVATION, AND RISK MANAGEMENT FOR AGENCY USE OF AI
            Requirement 1. By Q2 2024, all agencies must conduct AI inventory and risk assessments.
            Requirement 2. The Department of Commerce shall provide technical assistance to agencies.
            Requirement 3. Agencies must report AI use cases and risk mitigation strategies to OMB by December 31, 2024.
            Requirement 4. Establish AI governance committees in each agency with cross-functional representation."""
        ]
        
        example_titles = [
            "Executive Order on AI Safety",
            "DOE Strategic Plan 2023-2027", 
            "OMB AI Governance Memorandum"
        ]
        
        example_urls = [
            "https://www.whitehouse.gov/eo-14110-ai/",
            "https://www.energy.gov/strategic-plan-2023",
            "https://www.whitehouse.gov/omb/memorandum/m-24-10/"
        ]
        
        docs = []
        for i, (text, title, url) in enumerate(zip(example_texts, example_titles, example_urls)):
            docs.append(GovDocument(
                id=f"DOC_EX{i+1:03d}",
                title=title,
                source_url=url,
                issuing_authority="U.S. Government" if i == 0 else "Department of Energy" if i == 1 else "Office of Management and Budget",
                publication_date="October 30, 2023" if i == 0 else "March 15, 2023" if i == 1 else "January 26, 2024",
                document_type="Executive Order" if i == 0 else "Strategic Plan" if i == 1 else "OMB Memorandum",
                raw_text=text,
                crawl_timestamp=datetime.now().isoformat()
            ))
            
        return docs
    
    def _prepare_outputs(self,
                        documents: List[GovDocument],
                        components: List[AtomicComponent],
                        alignments: List[AlignmentVector],
                        gaps: List[Dict],
                        recommendations: List[AgencyRecommendation]) -> Dict:
        """Prepare all output formats"""
        
        # 1. Executive Dashboard (JSON)
        dashboard = {
            "summary": {
                "documents_analyzed": len(documents),
                "components_identified": len(components),
                "alignments_found": len(alignments),
                "gaps_detected": len(gaps),
                "recommendations_generated": len(recommendations),
                "timestamp": datetime.now().isoformat()
            },
            "top_alignments": [
                {
                    "component_a": next((c.text[:100] for c in components if c.id == a.component_a_id), ""),
                    "component_b": next((c.text[:100] for c in components if c.id == a.component_b_id), ""),
                    "strength": a.strength,
                    "evidence": a.evidence
                }
                for a in sorted(alignments, key=lambda x: x.strength, reverse=True)[:5]
            ],
            "top_gaps": gaps[:3],
            "priority_actions": [
                {
                    "agency": r.agency,
                    "priority": r.priority,
                    "action": r.recommended_action,
                    "timeline": r.timeline
                }
                for r in recommendations if r.priority == "HIGH"
            ]
        }
        
        # 2. Alignment Matrix (CSV-style structure)
        alignment_matrix = []
        doc_ids = sorted(set(c.document_id for c in components))
        
        for doc_a in doc_ids:
            row = {"document": doc_a}
            for doc_b in doc_ids:
                if doc_a == doc_b:
                    row[doc_b] = "SELF"
                else:
                    # Count alignments between these documents
                    count = sum(
                        1 for a in alignments
                        if any(c.document_id == doc_a for c in components if c.id == a.component_a_id) and
                        any(c.document_id == doc_b for c in components if c.id == a.component_b_id)
                    )
                    row[doc_b] = count
            alignment_matrix.append(row)
            
        # 3. Agency Action Memos (Markdown-ready)
        action_memos = {}
        for rec in recommendations:
            memo = f"""# ACTION MEMO: {rec.agency}
## Priority: {rec.priority}
### Recommended Action
{rec.recommended_action}

### Rationale
{rec.rationale}

### Timeline
{rec.timeline}

### Alignment Leverage
Can build upon {rec.alignment_leverage} existing alignment(s)

### Communication Points
{chr(10).join(f"- {point}" for point in rec.communication_points)}

### Implementation Notes
- Coordinate with relevant interagency partners
- Align with existing strategic plans
- Report progress through established channels
"""
            action_memos[rec.agency] = memo
            
        return {
            "executive_dashboard": dashboard,
            "alignment_matrix": alignment_matrix,
            "action_memos": action_memos,
            "raw_data": {
                "documents": [asdict(d) for d in documents],
                "components": [asdict(c) for c in components],
                "alignments": [asdict(a) for a in alignments],
                "gaps": gaps,
                "recommendations": [asdict(r) for r in recommendations]
            }
        }
    
    def save_outputs(self, outputs: Dict, output_dir: str = "outputs"):
        """Save all outputs to files"""
        Path(output_dir).mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save JSON dashboard
        with open(Path(output_dir) / f"dashboard_{timestamp}.json", 'w') as f:
            json.dump(outputs["executive_dashboard"], f, indent=2)
            
        # Save alignment matrix as CSV
        import csv
        with open(Path(output_dir) / f"alignment_matrix_{timestamp}.csv", 'w', newline='') as f:
            if outputs["alignment_matrix"]:
                writer = csv.DictWriter(f, fieldnames=outputs["alignment_matrix"][0].keys())
                writer.writeheader()
                writer.writerows(outputs["alignment_matrix"])
                
        # Save action memos as Markdown
        for agency, memo in outputs["action_memos"].items():
            safe_agency = agency.replace(' ', '_').replace(',', '')
            with open(Path(output_dir) / f"memo_{safe_agency}_{timestamp}.md", 'w') as f:
                f.write(memo)
                
        # Save raw data
        with open(Path(output_dir) / f"raw_data_{timestamp}.json", 'w') as f:
            json.dump(outputs["raw_data"], f, indent=2, default=str)
            
        self.logger.info(f"Outputs saved to {output_dir}/")

# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Governance Alignment Engine - Federal Policy Analysis System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --crawl --seeds https://www.whitehouse.gov/briefing-room/ https://www.energy.gov/policy
  %(prog)s --analyze --demo
  %(prog)s --full-pipeline --output-dir ./reports
        """
    )
    
    parser.add_argument('--crawl', action='store_true',
                       help='Run document crawling phase')
    parser.add_argument('--seeds', nargs='+',
                       default=['https://www.whitehouse.gov/briefing-room/',
                                'https://www.energy.gov/policy',
                                'https://www.commerce.gov/news'],
                       help='Seed URLs for crawling')
    parser.add_argument('--crawl-batch', type=int, default=10,
                       help='Number of URLs to crawl per batch')
    
    parser.add_argument('--analyze', action='store_true',
                       help='Run analysis on crawled documents')
    parser.add_argument('--doc-limit', type=int, default=5,
                       help='Maximum documents to analyze')
    
    parser.add_argument('--full-pipeline', action='store_true',
                       help='Run complete pipeline (crawl + analyze)')
    parser.add_argument('--demo', action='store_true',
                       help='Use example documents for demonstration')
    
    parser.add_argument('--output-dir', default='./gae_outputs',
                       help='Directory for output files')
    parser.add_argument('--db-path', default='governance.db',
                       help='Path to SQLite database')
    
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(levelname)s: %(message)s')
    
    # Initialize engine
    engine = GovernanceAlignmentEngine(db_path=args.db_path)
    
    if args.full_pipeline or (not args.crawl and not args.analyze):
        # Default: run full pipeline with demo data
        seed_urls = args.seeds if not args.demo else []
        
        outputs = engine.run_full_pipeline(
            seed_urls=seed_urls,
            crawl_batch_size=args.crawl_batch,
            analyze_document_limit=args.doc_limit
        )
        
        if outputs and "error" not in outputs:
            engine.save_outputs(outputs, args.output_dir)
            
            # Print executive summary
            print("\n" + "="*60)
            print("GOVERNANCE ALIGNMENT ENGINE - EXECUTIVE SUMMARY")
            print("="*60)
            
            dash = outputs["executive_dashboard"]
            print(f"\nDocuments Analyzed: {dash['summary']['documents_analyzed']}")
            print(f"Components Identified: {dash['summary']['components_identified']}")
            print(f"Alignments Found: {dash['summary']['alignments_found']}")
            print(f"Gaps Detected: {dash['summary']['gaps_detected']}")
            
            if dash['priority_actions']:
                print("\nHIGH PRIORITY ACTIONS:")
                for action in dash['priority_actions']:
                    print(f"  • {action['agency']}: {action['action']}")
                    
            print(f"\nFull outputs saved to: {args.output_dir}/")
            
    elif args.crawl:
        print("Running crawling phase...")
        engine.crawler.seed_frontier(args.seeds)
        new_docs = engine.crawler.crawl_batch(batch_size=args.crawl_batch)
        print(f"Crawled {len(new_docs)} new documents")
        
    elif args.analyze:
        print("Running analysis phase...")
        # For analysis-only, use example documents
        example_docs = engine._get_example_documents()
        
        all_components = []
        for doc in example_docs[:args.doc_limit]:
            components = engine.componentizer.componentize_document(doc)
            all_components.extend(components)
            print(f"Deconstructed {doc.title} → {len(components)} components")
            
        if len(all_components) >= 2:
            # Run analysis
            doc_components = {}
            for comp in all_components:
                doc_id = comp.document_id
                if doc_id not in doc_components:
                    doc_components[doc_id] = []
                doc_components[doc_id].append(comp)
                
            all_alignments = []
            doc_ids = list(doc_components.keys())
            
            for i in range(len(doc_ids)):
                for j in range(i+1, len(doc_ids)):
                    alignments = engine.analyzer.analyze_alignments(
                        doc_components[doc_ids[i]],
                        doc_components[doc_ids[j]]
                    )
                    all_alignments.extend(alignments)
                    
            print(f"\nFound {len(all_alignments)} alignment vectors")
            
            # Show top alignments
            if all_alignments:
                print("\nTOP ALIGNMENTS:")
                for i, align in enumerate(sorted(all_alignments, key=lambda x: x.strength, reverse=True)[:3]):
                    comp_a = next((c for c in all_components if c.id == align.component_a_id), None)
                    comp_b = next((c for c in all_components if c.id == align.component_b_id), None)
                    if comp_a and comp_b:
                        print(f"{i+1}. {comp_a.text[:80]}... → {comp_b.text[:80]}...")
                        print(f"   Strength: {align.strength:.2f} | Evidence: {align.evidence[:100]}...")
                        print()

if __name__ == "__main__":
    main()
