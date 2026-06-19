"""Pydantic models for the graph data."""
from pydantic import BaseModel
from typing import Optional
from enum import Enum
from datetime import date

class DocumentType(str, Enum):
    """Enumeration for document types."""
    CONSTITUTION_ARTICLE = "Constitution_Article"
    CONSTITUTION_SECTION = "Constitution_Section"
    CONSTITUTION_CLAUSE = "Constitution_Clause"
    USC_TITLE = "USC_Title"
    USC_CHAPTER = "USC_Chapter"
    USC_SECTION = "USC_Section"
    CFR_PART = "CFR_Part"
    CASE = "Case"
    OMB_MEMO = "OMB_Memo"
    USDA_DIRECTIVE = "USDA_Directive"
    EXECUTIVE_ORDER = "ExecutiveOrder"

class ProvisionLevel(str, Enum):
    """Enumeration for provision levels."""
    ARTICLE = "Article"
    SECTION = "Section"
    CLAUSE = "Clause"
    TITLE = "Title"
    CHAPTER = "Chapter"
    USC_SECTION = "USC_Section"
    CFR_SECTION = "CFR_Section"


class Document(BaseModel):
    """A legal or governmental document."""
    id: str
    name: str
    type: DocumentType
    number: Optional[str] = None
    categoryCode: Optional[str] = None
    date: Optional[date] = None

class Provision(BaseModel):
    """A specific provision within a document."""
    id: str
    citation: str
    level: ProvisionLevel
    heading: Optional[str] = None
    text: Optional[str] = None

class Institution(BaseModel):
    """An organization or institution."""
    id: str
    name: str
    kind: str

class DirectiveCategory(BaseModel):
    """A category for USDA directives."""
    code: str
    name: str
