#!/usr/bin/env python
# coding: utf-8

# # Exportera en Pandas `DataFrame` till SQL
# 
# Import modules

import pandas as pd
import sqlite3
import logging

# Create connection to database file, no matter load or save
con = sqlite3.connect('Uppland.db')
        
class DatabaseLoader:
    "Class to load data from SQL table."

    def __init__(self, connection=con) -> None:
        self.logger = logging.getLogger(__name__)
        self.connection = connection
    def load_data(self):
        self.logger.info('Loading data...')
        if pd.read_sql('SELECT name FROM sqlite_master', self.connection).empty:
            self.logger.error('Making new database due to error ...')
            return pd.DataFrame(columns=["signum", "revisionID", "translitterering", "normalisering", "translation", "edition"])
        else:
            self.logger.info('Load data: success.')
            return pd.read_sql('SELECT * FROM Upplands_runeinscriptions', self.connection)


