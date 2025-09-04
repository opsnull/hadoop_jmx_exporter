"""
Shared pytest fixtures and configuration for the test suite.
"""

import tempfile
import shutil
import json
from pathlib import Path
from unittest.mock import Mock, MagicMock
import pytest


@pytest.fixture
def temp_dir():
    """Provides a temporary directory that is cleaned up after the test."""
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    shutil.rmtree(temp_path)


@pytest.fixture
def mock_requests_response():
    """Mock requests.Response object for testing HTTP calls."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"test": "data"}
    mock_response.text = '{"test": "data"}'
    mock_response.headers = {"Content-Type": "application/json"}
    return mock_response


@pytest.fixture
def sample_jmx_response():
    """Sample JMX response data for testing."""
    return {
        "beans": [
            {
                "name": "Hadoop:service=NameNode,name=FSNamesystem",
                "modelerType": "FSNamesystem",
                "CapacityUsed": 12345,
                "CapacityRemaining": 67890,
                "TotalBlocks": 100
            }
        ]
    }


@pytest.fixture
def mock_prometheus_registry():
    """Mock Prometheus registry for testing metrics collection."""
    return MagicMock()


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "namenode_url": "http://localhost:9870",
        "datanode_url": "http://localhost:9864",
        "refresh_interval": 30,
        "metrics_port": 8000
    }


@pytest.fixture
def mock_yaml_config(tmp_path):
    """Creates a temporary YAML config file for testing."""
    config_data = {
        "hadoop": {
            "namenode_url": "http://test-namenode:9870",
            "datanode_url": "http://test-datanode:9864"
        },
        "exporter": {
            "port": 8000,
            "refresh_interval": 60
        }
    }
    config_file = tmp_path / "test_config.yaml"
    with open(config_file, 'w') as f:
        import yaml
        yaml.dump(config_data, f)
    return config_file


@pytest.fixture(autouse=True)
def reset_prometheus_registry():
    """Reset Prometheus registry before each test to avoid metric conflicts."""
    from prometheus_client import CollectorRegistry, REGISTRY
    # Clear the default registry
    collectors = list(REGISTRY._collector_to_names.keys())
    for collector in collectors:
        try:
            REGISTRY.unregister(collector)
        except KeyError:
            pass