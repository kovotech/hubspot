import os
from dotenv import load_dotenv
import sys
load_dotenv()
PROJECT_PATH=os.environ.get('PROJECT_PATH_SERVER')
sys.path.append(PROJECT_PATH)
from modules.fileLogger import PurgeFileLogs
from sqlops.tables.sql_logger import PurgeSQLLogs
from sqlops.engine import Engine

SQL_HOST=os.environ.get('SQL_HOST')
SQL_DB=os.environ.get('SQL_DB')
SQL_USER=os.environ.get('SQL_USER')
SQL_PSWD=os.environ.get('SQL_PSWD')

LOG_FOLDER_LIST=[
    f"{PROJECT_PATH}/pipelines/hubspot_traffic_sources_l1/Logs/cronjob",
    f"{PROJECT_PATH}/pipelines/hubspot_traffic_sources_l2/Logs/cronjob",
    f"{PROJECT_PATH}/pipelines/hubspot_traffic_sources_l3/Logs/cronjob"
]

AUTO_DELETION_MAX_DAYS=90

print("Purging File Logs...",flush=True)
purge_file_logs_pbj = PurgeFileLogs(LOG_FOLDER_LIST)
purge_file_logs_pbj.purge_by_days(days=AUTO_DELETION_MAX_DAYS)
print(f"File Logs older than {AUTO_DELETION_MAX_DAYS} are Purged...",flush=True)


print(f"=========================================================================",flush=True)

print("Purging SQL Logs...",flush=True)
sql_engine = Engine.mysql(
                            host=SQL_HOST,
                            db=SQL_DB,
                            user=SQL_USER,
                            pswd=SQL_PSWD
                            )
purge_sql_logs_pbj = PurgeSQLLogs(sql_engine,db="powerbi_python",table="middleware_logs")
purge_sql_logs_pbj.purge_by_days(days=AUTO_DELETION_MAX_DAYS,date_col="log_datetime")
print(f"File Logs older than {AUTO_DELETION_MAX_DAYS} are Purged...",flush=True)