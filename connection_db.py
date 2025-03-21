import duckdb
import pandas as pd
class Connection_db:
    def __init__(self, db_name):
        self.conn = duckdb.connect(db_name,config={'threads': 10})

    def create_table(self, table_name):
        self.conn.execute(f'CREATE TABLE {table_name} (url VARCHAR)')
        self.conn.commit()    
    

    def get_links(self):
        rows = self.conn.execute('SELECT * FROM data').fetchall()
        print(f'Found {len(rows)} links')
        return [row[0] for row in rows] 
    
    def insert_links(self, table_name, links):
        self.conn.executemany(f'INSERT INTO {table_name} VALUES (?)', [[l] for l in links])
        self.conn.commit()
        print(f'Inserted {len(links)} links')

    def close_connection(self):
        self.conn.close()

    def get_connection(self) :
        return self.conn
    
