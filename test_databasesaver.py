import pytest
import pandas as pd
import sqlite3
from unittest import mock
#from case import mock
from io import StringIO

from databasesaver import DatabaseSave, DatabaseLoader, con


@pytest.fixture
def sample_dataframe():
    """Fixture that returns a sample DataFrame."""
    data = StringIO("""
    id,name,description
    1,Stone A,Runic inscription A
    2,Stone B,Runic inscription B
    """)
    df = pd.read_csv(data)
    return df


@mock.patch('databasesaver.pd.DataFrame.to_sql')
def test_save_data(mock_to_sql, sample_dataframe):
    """Test saving data to the database."""
    # Mocking to_sql to avoid actual database I/O
    db_saver = DatabaseSave(sample_dataframe)

    # Act: Call the method to save the data
    db_saver.save_data()

    # Assert: Check that to_sql was called with the correct arguments
    mock_to_sql.assert_called_once_with('Upplands_runeinscriptions', con, if_exists='replace')

