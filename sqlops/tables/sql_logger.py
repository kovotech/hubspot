from sqlalchemy import MetaData, Table, Column, BigInteger, Text, DateTime, text
import sqlalchemy
from sqlalchemy.schema import CreateTable
import datetime as dt

class SQLLogger:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "middleware_logs", MetaData(),
            Column("id",BigInteger,primary_key=True,autoincrement=True),
            Column("middleware_name",Text),
            Column("job_name",Text),
            Column("job_type",Text),
            Column("table_name",Text),
            Column("log_type",Text),
            Column("log_description",Text),
            Column("log_datetime",DateTime)
        )
        return TABLE
    
    def create_table(self):
        table = SQLLogger.getTable()
        stmt = CreateTable(table,if_not_exists=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    @staticmethod
    def map(middleware_name:str,job_name:str,job_type:str,table_name:str,log_type:str,log_description:str):
        current_datetime = dt.datetime.now()
        log_dict = {}
        log_dict['id'] = None
        log_dict['middleware_name'] = middleware_name
        log_dict['job_name'] = job_name
        log_dict['job_type'] = job_type
        log_dict['table_name'] = table_name
        log_dict['log_type'] = log_type
        log_dict['log_description'] = log_description
        log_dict['log_datetime'] = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

        return log_dict
    
    def import_to_sql(self,middleware_name:str,job_name:str,job_type:str,table_name:str,log_type:str,log_description:str):
        payload = SQLLogger.map(middleware_name,job_name,job_type,table_name,log_type,log_description)
        table = SQLLogger.getTable()
        stmt = table.insert().values(payload)
        with self.engine.begin() as connx:
            connx.execute(stmt)


class PurgeSQLLogs:
    def __init__(self,engine:sqlalchemy.engine.Engine,db:str,table:str) -> None:
        self.engine=engine
        self.db=db
        self.table=table

    def purge_by_days(self,days:int,date_col:str):
        current_date = dt.datetime.today()
        last_date = (current_date-dt.timedelta(days=days)).strftime("%Y-%m-%d")
        stmt = text(f"""DELETE FROM `{self.db}`.`{self.table}` WHERE {date_col} < '{last_date}'""")
        print(stmt,flush=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    def purge_by_date_range(self,start_date:str,end_date:str,date_col:str):
        stmt = text(f"""DELETE FROM `{self.db}`.`{self.table}` WHERE {date_col} >= '{start_date} 00:00:00' and log_datetime <= '{end_date} 23:59:59'""")
        print(stmt,flush=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)