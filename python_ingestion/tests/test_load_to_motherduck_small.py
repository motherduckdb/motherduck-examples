import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from python_ingestion.load_to_motherduck_small import fetch_github_data, process_data

@pytest.fixture
def sample_github_data():
    return [
        {
            'author': {'login': 'user1'},
            'weeks': [{'c': 5}, {'c': 3}, {'c': 2}]
        },
        {
            'author': {'login': 'user2'},
            'weeks': [{'c': 1}, {'c': 4}, {'c': 6}]
        }
    ]

def test_fetch_github_data():
    with patch('load_to_motherduck_small.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = [{'author': {'login': 'user1'}, 'weeks': [{'c': 5}]}]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        data = fetch_github_data()
        assert data == [{'author': {'login': 'user1'}, 'weeks': [{'c': 5}]}]
        mock_get.assert_called_once_with('https://api.github.com/repos/duckdb/duckdb/stats/contributors')

def test_process_data(sample_github_data):
    df = process_data(sample_github_data)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)
    assert list(df.columns) == ['login', 'total_commits']
    assert df.iloc[0]['login'] == 'user1'
    assert df.iloc[0]['total_commits'] == 10
    assert df.iloc[1]['login'] == 'user2'
    assert df.iloc[1]['total_commits'] == 11
