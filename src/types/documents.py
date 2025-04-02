# src/types/documents.py
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum


class DocumentType(str, Enum):
    PDF = "pdf"
    TEXT = "text"
    JSON = "json"


class ExtractedDocument(BaseModel):
    """Represents a document with extracted text."""

    document_id: str
    filename: str
    document_type: DocumentType
    extraction_date: datetime = Field(default_factory=datetime.now)
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StructuredDocument(BaseModel):
    """Represents a document with structured data extracted."""

    document_id: str
    extraction_date: datetime = Field(default_factory=datetime.now)
    filename: str
    structured_data: Dict[str, Any]
    processing_metadata: Dict[str, Any] = Field(default_factory=dict)
