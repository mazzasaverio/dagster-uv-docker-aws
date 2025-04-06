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


class ESGReportData(BaseModel):
    """Schema for structured ESG report data extraction."""

    company_name: str
    report_year: int
    environmental_metrics: Dict[str, Any] = Field(
        description="Environmental metrics and initiatives from the report"
    )
    social_metrics: Dict[str, Any] = Field(
        description="Social metrics and initiatives from the report"
    )
    governance_metrics: Dict[str, Any] = Field(
        description="Governance metrics and practices from the report"
    )
    sustainability_goals: List[str] = Field(
        description="Key sustainability goals mentioned in the report"
    )
    carbon_emissions: Optional[Dict[str, Any]] = Field(
        description="Carbon emissions data if available", default=None
    )
    key_initiatives: List[str] = Field(
        description="Key ESG initiatives described in the report"
    )
    executive_summary: str = Field(
        description="Brief executive summary of the ESG report"
    )
