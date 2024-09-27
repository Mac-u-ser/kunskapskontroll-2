import pytest
import pandas as pd
from io import StringIO
from database_loader import DatabaseLoader

@pytest.fixture
def sample_dataframe():
    """Fixture that returns a sample DataFrame representing data from the SQL table."""
    return pd.DataFrame({
        "signum": [1, 2],
        "revisionID": [101, 102],
        "translitterering": ["abc", "def"],
        "normalisering": ["ghi", "jkl"],
        "translation": ["Translation 1", "Translation 2"],
        "edition": ["Edition 1", "Edition 2"]
    })

def test_load_data_empty_database(mocker):
    """Test the behavior when the database has no tables (returns an empty DataFrame)."""
    # Arrange: Simulate an empty result when querying the sqlite_master table
    mock_read_sql = mocker.patch('database_loader.pd.read_sql', side_effect=[
        pd.DataFrame(),  # No tables exist in the database
        pd.DataFrame()   # Simulating the load data as empty
    ])
    
    db_loader = DatabaseLoader()

    # Act: Call the method to load data
    result = db_loader.load_data()

    # Assert: Check if it returns an empty DataFrame with the correct columns
    expected_columns = ["signum", "revisionID", "translitterering", "normalisering", "translation", "edition"]
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == expected_columns
    assert result.empty  # Should be an empty DataFrame

    # Check if logging was called with the appropriate message
    mock_logger_error = mocker.patch.object(db_loader.logger, 'error')
    db_loader.load_data()
    mock_logger_error.assert_called_once_with('Making new database due to error ...')


def test_load_data_success(mocker, sample_dataframe):
    """Test the behavior when the table exists and data is loaded successfully."""
    # Arrange: Simulate the table existence and the actual table data
    mock_read_sql = mocker.patch('database_loader.pd.read_sql', side_effect=[
        pd.DataFrame({'name': ['Upplands_runeinscriptions']}),  # Simulating the table exists
        sample_dataframe  # Returning actual data from the table
    ])

    db_loader = DatabaseLoader()

    # Act: Call the method to load data
    result = db_loader.load_data()

    # Assert: Check if the loaded data matches the sample data
    pd.testing.assert_frame_equal(result, sample_dataframe)

    # Check if logging was called with the appropriate message
    mock_logger_info = mocker.patch.object(db_loader.logger, 'info')
    db_loader.load_data()
    mock_logger_info.assert_any_call('Load data: success.')
