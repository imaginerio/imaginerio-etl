"""Tests for the Item class."""

import pytest
from iiif_prezi3 import KeyValueString

from imaginerio_etl.entities.item import Item


def test_item_initialization(sample_metadata_row, sample_vocabulary):
    """Test basic Item initialization."""
    item = Item("TEST001", sample_metadata_row, sample_vocabulary)
    
    assert item._id == "TEST001"
    assert item._title == "Test Image"
    assert item._description == {
        "en": ["Test Description EN"],
        "pt-BR": ["Test Description PT"]
    }


def test_map_wikidata_with_valid_term(sample_metadata_row, sample_vocabulary):
    """Test map_wikidata with a term that has both Wikidata ID and Portuguese translation."""
    item = Item("TEST001", sample_metadata_row, sample_vocabulary)
    result = item.map_wikidata(
        {"en": ["Test"], "pt-BR": ["Teste"]},
        "Test Term"
    )
    
    assert isinstance(result, KeyValueString)
    assert len(result.value["en"]) == 1
    assert len(result.value["pt-BR"]) == 1
    assert 'Q123' in result.value["en"][0]  # Check Wikidata ID in link
    assert 'Termo Teste' in result.value["pt-BR"][0]  # Check Portuguese translation


def test_map_wikidata_with_missing_wikidata(sample_metadata_row, sample_vocabulary):
    """Test map_wikidata with a term that has no Wikidata ID."""
    item = Item("TEST001", sample_metadata_row, sample_vocabulary)
    result = item.map_wikidata(
        {"en": ["Test"], "pt-BR": ["Teste"]},
        "No Wikidata"
    )
    
    assert isinstance(result, KeyValueString)
    assert result.value["en"] == ["No Wikidata"]
    assert result.value["pt-BR"] == ["Sem Wikidata"]


def test_map_wikidata_with_missing_portuguese(sample_metadata_row, sample_vocabulary):
    """Test map_wikidata with a term that has no Portuguese translation."""
    item = Item("TEST001", sample_metadata_row, sample_vocabulary)
    result = item.map_wikidata(
        {"en": ["Test"], "pt-BR": ["Teste"]},
        "No Portuguese"
    )
    
    assert isinstance(result, KeyValueString)
    assert 'Q456' in result.value["en"][0]  # Check Wikidata ID in link
    assert 'No Portuguese' in result.value["pt-BR"][0]  # Should fall back to English


def test_map_wikidata_with_empty_portuguese(sample_metadata_row, sample_vocabulary):
    """Test map_wikidata with a term that has pd.NA as Portuguese translation."""
    item = Item("TEST001", sample_metadata_row, sample_vocabulary)
    result = item.map_wikidata(
        {"en": ["Test"], "pt-BR": ["Teste"]},
        "Empty Portuguese"
    )
    
    assert isinstance(result, KeyValueString)
    assert 'Q789' in result.value["en"][0]  # Check Wikidata ID in link
    assert 'Empty Portuguese' in result.value["pt-BR"][0]  # Should fall back to English


def test_map_wikidata_with_multiple_terms(sample_metadata_row, sample_vocabulary):
    """Test map_wikidata with multiple pipe-separated terms."""
    item = Item("TEST001", sample_metadata_row, sample_vocabulary)
    result = item.map_wikidata(
        {"en": ["Test"], "pt-BR": ["Teste"]},
        "Test Term|No Wikidata"
    )
    
    assert isinstance(result, KeyValueString)
    assert len(result.value["en"]) == 2
    assert len(result.value["pt-BR"]) == 2
    assert 'Q123' in result.value["en"][0]  # First term should have Wikidata link
    assert 'No Wikidata' == result.value["en"][1]  # Second term should be plain text


def test_map_wikidata_with_nonexistent_term(sample_metadata_row, sample_vocabulary):
    """Test map_wikidata with a term that doesn't exist in vocabulary."""
    item = Item("TEST001", sample_metadata_row, sample_vocabulary)
    result = item.map_wikidata(
        {"en": ["Test"], "pt-BR": ["Teste"]},
        "Nonexistent Term"
    )
    
    assert result is None  # Should return None when no valid terms are found


def test_map_wikidata_with_empty_input(sample_metadata_row, sample_vocabulary):
    """Test map_wikidata with empty input."""
    item = Item("TEST001", sample_metadata_row, sample_vocabulary)
    result = item.map_wikidata(
        {"en": ["Test"], "pt-BR": ["Teste"]},
        ""
    )
    
    assert result is None  # Should return None for empty input 


def test_dimension_conversion_with_number(sample_metadata_row, sample_vocabulary):
    """Test format_dimension with a number"""
    item = Item("Test0001", sample_metadata_row, sample_vocabulary)
    result = item._format_dimension(100)

    assert "10 cm" in result


def test_dimension_conversion_with_string(sample_metadata_row, sample_vocabulary):
    """Test format_dimension with a string"""
    item = Item("Test0001", sample_metadata_row, sample_vocabulary)
    result = item._format_dimension("a string")

    assert "a string" in result