# -*- coding: utf-8 -*-

import sqlite3

class SQLDb():
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()

    def table_exists(self, name):
        self.cursor.execute("SELECT EXISTS(SELECT name FROM sqlite_master WHERE type='table' AND name='{}');".format(name))
        return self.cursor.fetchone()[0]

    def row_exists(self, table, condition):
        self.cursor.execute("SELECT EXISTS(SELECT * FROM {} WHERE {} LIMIT 1);".format(table, condition))
        return self.cursor.fetchone()[0]

    def execute(self, command):
        self.cursor.execute(command)
        return self.cursor.fetchone()

    def execute_group(self, command):
        self.cursor.execute(command)
        return self.cursor.fetchall()
