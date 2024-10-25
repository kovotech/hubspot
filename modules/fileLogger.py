import logging
import datetime as dt
import traceback
import datetime as dt
import os
from dateutil.parser import parse

def myLogger(level,msg,folder_path):
    
    if os.path.exists(folder_path):
        pass
    else:
        os.makedirs(folder_path)
        pass
    
    current_date = dt.datetime.strftime(dt.datetime.today(),'%Y-%m-%d')
    logging.basicConfig(filename=f"{folder_path}/{current_date}.log",
                        format='%(asctime)s %(levelname)s %(message)s')

    logger = logging.getLogger()

    logger.setLevel(logging.DEBUG)

    getattr(logger,level)(msg)


def format_exception_logfile(exception:Exception):
    traceback_list = traceback.format_tb(exception.__traceback__)
    tb_str_logfile = ""
    for i in traceback_list:
        str_tuple = str(i).split(",")
        tb_str_logfile += f"\n{str_tuple[0]} in {str_tuple[1]}"
    return f"Exception:{exception.__repr__()}\nTraceback:{tb_str_logfile}"


class PurgeFileLogs:
    def __init__(self,folder_path_list:list) -> None:
        self.folder_path_list=folder_path_list

    def purge_by_days(self,days:int):
        for folder_path in self.folder_path_list:
            last_date = dt.datetime.today()-dt.timedelta(days=days)
            for file in os.listdir(folder_path):
                file_name,file_ext = os.path.splitext(file)
                if file_ext == '.log':
                    file_date = parse(file_name)
                    if file_date < last_date:
                        file_to_be_deleted = f"{folder_path}/{file}"
                        os.remove(file_to_be_deleted)

        return None

    def purge_by_date_range(self,start_date:str,end_date:str):
        for folder_path in self.folder_path_list:
            # print("===========================================",flush=True)
            start_date_ = parse(start_date)
            end_date_ = parse(end_date)
            print(f"StartDate:{start_date_}|EndDate:{end_date_}")
            for file in os.listdir(folder_path):
                # print(f"{file} is a file...",flush=True)
                # print("--------------------",flush=True)
                file_name,file_ext = os.path.splitext(file)
                print(f"filename is {file_name} and ext is {file_ext}...",flush=True)
                print("--------------------",flush=True)
                if file_ext == '.log':
                    file_date = parse(file_name)
                    if file_date >= start_date_ and file_date <= end_date_:
                        file_to_be_deleted = f"{folder_path}/{file}"
                        os.remove(file_to_be_deleted)
        return None