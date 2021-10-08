import snowflake.connector
import pandas as pd
import traceback
import os


# Make the wrapper object a Borg for maximal reuse of the connection
class Borg:
    __shared_state = {}

    def __init__(self):
        self.__dict__ = self.__shared_state


class LocalSnowflakeWrapper(Borg):

    def __init__(self, user: str, region: str = "eu-central-1", schema: str = "SANDBOX_ALL"):
        super().__init__()
        self.user = user
        self.region = region
        self.schema = schema

    def get_pandas_df(self, sql: str) -> pd.DataFrame:
        try:
            df = pd.read_sql_query(sql, self.conn)
            return df
        except:
            print(traceback.format_exc())

    def custom_fetch_db(self, sql: str) -> pd.DataFrame:
        sql = sql.replace("{{SCHEMA}}", self.schema)
        df = self.get_pandas_df(sql)
        df.columns = map(str.lower, df.columns)
        return df

    def write(self, df: pd.DataFrame = None, table: str = None, clean_data=False):
        """
        Create a table with declared column names and types in Snowflake.

        :params
        -------
        df : dataframe
            Dataframe with your dataset.
        table : str
            Table to write to.
        clean_data: boolean
            Removing commas and multiple white spaces.

        :return
        -------
        None
            Data loaded to database.
        """
        table = table.replace("{{SCHEMA}}", self.schema)
        if clean_data:
            df = df.replace(to_replace=',', value='', regex=True)
            df = df.replace(to_replace=r'\\', value='', regex=True)
        df.to_csv('my_df.csv', index=False, encoding='utf-8', header=False)
        print(f"this ---> create or replace stage {self.schema}.{table} file_format = (type = 'CSV' field_delimiter = ',');")
        self.run(
            f"create or replace stage {self.schema}.{table} file_format = (type = 'CSV' field_delimiter = ',');")
        self.run(f"PUT file://my_df.csv @{self.schema}.{table}")
        self.run(
            f"copy into {self.schema}.{table} from @{self.schema}.{table} file_format = (type = 'CSV' field_delimiter = ',') ;")
        os.remove('my_df.csv')
        return

    @property
    def conn(self):
        # lazy instantiation
        if not hasattr(self, "_conn"):
            self._conn = snowflake.connector.connect(
                account="rt27428",
                region=self.region,
                user=self.user,
                schema=self.schema,
                database="SANDBOX_DB",
                warehouse="ANALYSTS",
                authenticator="externalbrowser",
                autocommit=True,
            )
        return self._conn

    def get_conn(self):
        return self.conn

    def run(self, sql: str, autocommit: bool = True):
        self.conn.cursor().execute(sql)
        return

    def query(self, sql: str):
        sql = sql.replace("{{SCHEMA}}", self.schema)
        self.run(sql)
        return

    def pd_read_sql(self, sql_query: str, **kwargs):
        """
            Purpose:
            --------------------------------------------------------------------
            Executes extraction of data through pd.read_sql
             Input:
            --------------------------------------------------------------------
                sql_query: Sql statement to execute against database.
                           Note: tested only with SELECT statements!!!
                **kwargs: refer to pd.read_sql() description for the available
                          parameters
              Output:
            -------------------------------------------------------------------
                pd.DataFrame
        """
        conn = self.get_conn()
        result_df = pd.read_sql(sql=sql_query,
                                con=conn,
                                **kwargs)
        return result_df
