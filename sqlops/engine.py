from sqlalchemy import create_engine

class Engine:
    @staticmethod
    def mysql(host:str,db:str,user:str,pswd:str):
        engine = create_engine(f"mysql+pymysql://{user}:{pswd}@{host}/{db}")
        return engine