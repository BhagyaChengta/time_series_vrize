import snowflake as sf
from typing import Protocol, Union
import mysql.connector
import logging


class DbMethods(Protocol):

    def check_connection(self) -> tuple[bool, str]:
        ...

    def get_table_data(self, table: str, columns: list[str], conn):
        ...

    def run_query(self, conn, query) -> list[tuple]:
        ...


class Dbbase(DbMethods):
    def __init__(self, username, password, port=None, database=None, db_schema=None) -> None:
        self.username = username
        self.password = password
        self.port = port
        self.database = database
        self.schema = db_schema

    def run_query(self, conn, query):
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results

    def get_table_data(self, table, columns, conn):
        column_str = ",".join(columns)
        query = f"select {column_str} from {table};"
        output = self.run_query(conn, query)

        return


class SnowflakeConnector(Dbbase):

    def __init__(self, username, password, account, port=None, warehouse=None, database=None, db_schema=None,storage_integration=None) -> None:
        super(SnowflakeConnector,self).__init__(username, password, port, database, db_schema)
        self.account = account
        self.warehouse = warehouse

    def check_connection(self):
        try:
            conn = sf.connect(user=self.username, password=self.password,
                              account=self.account)
            logging.info("Connected to Snowflake successfully")

            return True , ''
        except Exception as e:
            logging.error(f"Error occurred while connecting to Snowflake: {str(e)}")
            return False , str(e)
    
    def get_connection(self):
        try:
            conn = sf.connect(user=self.username, password=self.password,
                              account=self.account)
            logging.info("Connected to Snowflake successfully")

            return conn
        except Exception as e:
            logging.error(f"Error occurred while connecting to Snowflake: {str(e)}")
            return False , str(e)
        

class MySQLConnector(Dbbase):
    def __init__(self, username, password, host, port=None, database=None) -> None:
        super(MySQLConnector, self).__init__(username, password, port, database)
        self.host = host

    def check_connection(self):
        try:
            conn = mysql.connector.connect(
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )

            logging.info("Connected to MySQL successfully")
            return True, ''
        except Exception as e:
            logging.error(f"Error occurred while connecting to MySQL: {str(e)}")
            return False, str(e)

    def get_connection(self):
        try:
            conn = mysql.connector.connect(
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
              
            logging.info("Connected to MySQL successfully")
            return conn
        except Exception as e:
            logging.error(f"Error occurred while connecting to MySQL: {str(e)}")
            return False, str(e)

