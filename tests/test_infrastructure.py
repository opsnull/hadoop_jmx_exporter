"""
Infrastructure validation tests to ensure testing setup is working correctly.
"""

import pytest
import sys
from pathlib import Path


def test_python_version():
    """Test that we're running on a supported Python version."""
    assert sys.version_info >= (3, 8), "Python 3.8+ is required"


def test_project_structure():
    """Test that the project has the expected structure."""
    project_root = Path(__file__).parent.parent
    
    # Check for main Python files
    assert (project_root / "hadoop_jmx_exporter.py").exists()
    assert (project_root / "requirements.txt").exists()
    assert (project_root / "pyproject.toml").exists()
    
    # Check testing structure
    assert (project_root / "tests").exists()
    assert (project_root / "tests" / "__init__.py").exists()
    assert (project_root / "tests" / "conftest.py").exists()
    assert (project_root / "tests" / "unit").exists()
    assert (project_root / "tests" / "integration").exists()


def test_imports():
    """Test that main dependencies can be imported."""
    try:
        import requests
        import prometheus_client
        import yaml
        assert True
    except ImportError as e:
        pytest.fail(f"Failed to import required dependency: {e}")


def test_pytest_markers():
    """Test that custom pytest markers are available."""
    # This test itself validates that pytest is working
    assert True


@pytest.mark.unit
def test_unit_marker():
    """Test the unit test marker."""
    assert True


@pytest.mark.integration  
def test_integration_marker():
    """Test the integration test marker."""
    assert True


@pytest.mark.slow
def test_slow_marker():
    """Test the slow test marker."""
    assert True


def test_fixtures_available(temp_dir, sample_config, mock_requests_response):
    """Test that shared fixtures are working."""
    # Test temp_dir fixture
    assert temp_dir.exists()
    assert temp_dir.is_dir()
    
    # Test sample_config fixture
    assert isinstance(sample_config, dict)
    assert "namenode_url" in sample_config
    
    # Test mock_requests_response fixture
    assert mock_requests_response.status_code == 200
    assert mock_requests_response.json() == {"test": "data"}