import os
from dotenv import load_dotenv
import sys
load_dotenv()
PROJECT_PATH=os.environ.get('PROJECT_PATH_SERVER')
sys.path.append(PROJECT_PATH)
from modules.api import HubSpotApiCredentials, HubSpotSourceApi, SerializeHubspotResponse
from sqlops.tables.hubspot_traffic_sources_l2 import Traffic_Source_L2, get_level1_data_labels
from sqlops.engine import Engine
from sqlops.tables.sql_logger import SQLLogger
from modules.fileLogger import *
from datetime import datetime as dt
import datetime
from dateutil import parser
import json
from modules.sendgrid import email_trigger, SendgridCredentials, EmailTriggerFields, format_exception_email

# SQL Credentials
SQL_HOST=os.environ.get('SQL_HOST')
SQL_DB=os.environ.get('SQL_DB')
SQL_USER=os.environ.get('SQL_USER')
SQL_PSWD=os.environ.get('SQL_PSWD')

# previous_date = dt.today()-datetime.timedelta(days=2)
# previous_date_str = dt.strftime(previous_date,'%Y-%m-%d')
START_DATE='2023-01-01'
END_DATE='2024-06-29'

MIDDLEWARE_NAME="HubSpot"
JOB_NAME="Traffic By Source L2"
JOB_TYPE=f"BulkImport [{START_DATE}-{END_DATE}]"
TABLENAME="powerbi_python.hubsot_traffic_sources_l2"
EMAIL_LIST=os.environ['EMAIL_TRIGGER_EMAIL_LIST']


LOG_FILE_PATH=f"{os.getcwd()}/Logs/bulkimport"

# SQL Logger
sql_engine = Engine.mysql(
                            host=SQL_HOST,
                            db=SQL_DB,
                            user=SQL_USER,
                            pswd=SQL_PSWD
                            )
sql_logger = SQLLogger(sql_engine)

# Email Trigger Configuration
sendgrid_credentials = SendgridCredentials(
                                        secret_key=os.environ['SENDGRID_SECRET_KEY'],
                                        from_email=os.environ['SENDGRID_FROM_EMAIL']
                                    )


def main():
    #==================================== Getting Data from HubSpot API ====================================
    start_date_ = parser.parse(START_DATE)
    end_date_ = parser.parse(END_DATE)
    all_api_response_data = []
    sql_engine = Engine.mysql(
                            host=SQL_HOST,
                            db=SQL_DB,
                            user=SQL_USER,
                            pswd=SQL_PSWD
                            )
    while start_date_ <= end_date_:
        print("===============================================",flush=True)
        start_date_str = dt.strftime(start_date_,'%Y-%m-%d')
        start_date_str2 = dt.strftime(start_date_,'%Y%m%d')
        label_list = get_level1_data_labels(date=start_date_str2,sql_engine=sql_engine)
        # print(label_list)
        print(f"Calling HubSpot traffic by source L2 data for date: {start_date_str}")
        for label in label_list:
            hubspot_credentials = HubSpotApiCredentials(
                                            os.environ['HUBSPOT_BASE_URL'],"analytics/v2/reports/sources/total",os.environ['HUBSPOT_TOKEN']
                                        )
            response = HubSpotSourceApi.get_total(
                                            credentials=hubspot_credentials,
                                            date=start_date_str2,
                                            drill_down_value1=label
                                        )
            # with open(f'test.json','w') as f:
            #     json.dump(response,fp=f,indent=3)
            print(f'Serializing {label} HubSpot API response to python dictionary...',flush=True)
            temp_data1 = SerializeHubspotResponse.serialize_totals(response,start_date_str)
            temp_data_final = SerializeHubspotResponse.add_key_value(temp_data1,{"parent_breakdown":label})
            for record in temp_data_final:
                all_api_response_data.append(record)
        # with open(f'{PROJECT_PATH}/samples/{start_date_str}.json','w') as f:
        #     json.dump(all_api_response_data,fp=f,indent=3)
        start_date_ += datetime.timedelta(days=1)

    #==================================== Getting Data from GA4 API ====================================
    #==================================== Importing Data to SQL ====================================
    print('Initiating SQL Engine...',flush=True)
    sql_engine = Engine.mysql(
                            host=SQL_HOST,
                            db=SQL_DB,
                            user=SQL_USER,
                            pswd=SQL_PSWD
                            )
    sql_obj = Traffic_Source_L2(engine=sql_engine)
    print('SQL Import Job Started...',flush=True)
    import_count = 1
    for record in all_api_response_data:
        sql_obj.import_to_sql(record)
        print(f'Records Imported {import_count}',end='\r',flush=True)
        import_count += 1
    # #==================================== Importing Data to SQL ====================================
    print(f"Total Records Imported: {import_count-1}")
    return import_count-1

if __name__ == '__main__':
    try:
        imported_records_count = main()
        sql_logger.import_to_sql(
                                middleware_name=MIDDLEWARE_NAME,
                                job_name=JOB_NAME,
                                job_type=JOB_TYPE,
                                table_name=TABLENAME,
                                log_type="INFO",
                                log_description=f"Successfully Executed | Records Imported: {imported_records_count}"
                                )
        myLogger(level='info',msg=f'{imported_records_count} imported to hubsot_traffic_sources_l2 table',folder_path=LOG_FILE_PATH)
    except Exception as e:
        formatted_log = format_exception_logfile(e)
        sql_logger.import_to_sql(
                                middleware_name=MIDDLEWARE_NAME,
                                job_name=JOB_NAME,
                                job_type=JOB_TYPE,
                                table_name=TABLENAME,
                                log_type="ERROR",
                                log_description=formatted_log
                                )
        myLogger(level='error',msg=formatted_log,folder_path=LOG_FILE_PATH)
        email_trigger_config = EmailTriggerFields(
                                                Subject="Test Company Middleware Notification",
                                                MiddlewareName=MIDDLEWARE_NAME,
                                                JobName=JOB_NAME,
                                                JobType=JOB_TYPE,
                                                TableName=TABLENAME,
                                                LogType="Error",
                                                LogDescription=format_exception_email(e)
                                                )
        email_trigger(EMAIL_LIST,email_trigger_config,sendgrid_credentials)
# main()