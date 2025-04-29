"""Common test fixtures for imaginerio-etl."""

import pytest
import pandas as pd


@pytest.fixture
def sample_vocabulary():
    """Sample vocabulary dictionary for testing."""
    return {
        "Test Term": {
            "Wikidata ID": "Q123",
            "Label (pt)": "Termo Teste",
        },
        "No Wikidata": {
            "Wikidata ID": None,
            "Label (pt)": "Sem Wikidata",
        },
        "No Portuguese": {
            "Wikidata ID": "Q456",
            "Label (pt)": None,
        },
        "Empty Portuguese": {
            "Wikidata ID": "Q789",
            "Label (pt)": pd.NA,
        }
    }


@pytest.fixture
def sample_metadata_row():
    """Sample metadata row for testing Item initialization."""
    return {
        "Title": "Test Image",
        "Description (English)": "Test Description EN",
        "Description (Portuguese)": "Test Description PT",
        "Document ID": "TEST001",
        "Creator": "Test Creator",
        "Date": "2024",
        "Depicts": "Test Term|No Wikidata",
        "Type": "Test Type",
        "Material": "Test Material",
        "Fabrication Method": "Test Method",
        "Width": "100",
        "Height": "200",
        "Required Statement": "Test Statement",
        "Rights": "Public Domain",
        "Document URL": "https://test.com/doc",
        "Provider": "Test Provider",
        "Wikidata ID": "Q999",
        "Smapshot ID": "S123",
        "Collection": "Test Collection",
        "Media URL": "https://test.com/media"
    } 