from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable
import pandas as pd

class Traffic_Source_L3:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "hubsot_traffic_sources_l3",MetaData(),
            Column('id',String(500),primary_key=True),
            Column('date',Date),
            Column('breakdown',Text),
            Column('parent_breakdown',Text),
            Column('sub_parent_breakdown',Text),
            Column('contactsPerPageview',DECIMAL(20,3)),
            Column('returningVisits',BigInteger),
            Column('rawViews',BigInteger),
            Column('contactToCustomerRate',DECIMAL(20,3)),
            Column('standardViews',BigInteger),
            Column('customersPerPageview',DECIMAL(20,3)),
            Column('sessionToContactRate',DECIMAL(20,3)),
            Column('pageviewsPerSession',DECIMAL(20,3)),
            Column('opportunities',BigInteger),
            Column('bounceRate',DECIMAL(20,3)),
            Column('visits',BigInteger),
            Column('visitors',BigInteger),
            Column('pageviewsMinusExits',BigInteger),
            Column('leads',BigInteger),
            Column('leadsPerView',DECIMAL(20,3)),
            Column('customers',BigInteger),
            Column('bounces',BigInteger),
            Column('time',BigInteger),
            Column('timePerSession',DECIMAL(20,3)),
            Column('contacts',BigInteger),
            Column('newVisitorSessionRate',DECIMAL(20,3))
        )
        return TABLE
    
    def create_table(self):
        table = Traffic_Source_L3.getTable()
        stmt = CreateTable(table,if_not_exists=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    @staticmethod
    def map(src:dict):
        output_dict:dict = {}
        try:
            output_dict['id'] = str(src['parent_breakdown'])+"_"+str(src['sub_parent_breakdown'])+"_"+(src['breakdown'])+"_"+str(src['date'])
        except:
            output_dict['id'] = None
        try:
            output_dict['date'] = src['date']
        except:
            output_dict['date'] = None
        try:
            output_dict['breakdown'] = src['breakdown']
        except:
            output_dict['breakdown'] = None
        try:
            output_dict['parent_breakdown'] = src['parent_breakdown']
        except:
            output_dict['parent_breakdown'] = None
        try:
            output_dict['sub_parent_breakdown'] = src['sub_parent_breakdown']
        except:
            output_dict['sub_parent_breakdown'] = None
        try:
            output_dict['contactsPerPageview'] = src['contactsPerPageview']
        except:
            output_dict['contactsPerPageview'] = None
        try:
            output_dict['returningVisits'] = src['returningVisits']
        except:
            output_dict['returningVisits'] = None
        try:
            output_dict['rawViews'] = src['rawViews']
        except:
            output_dict['rawViews'] = None
        try:
            output_dict['contactToCustomerRate'] = src['contactToCustomerRate']
        except:
            output_dict['contactToCustomerRate'] = None
        try:
            output_dict['standardViews'] = src['standardViews']
        except:
            output_dict['standardViews'] = None
        try:
            output_dict['customersPerPageview'] = src['customersPerPageview']
        except:
            output_dict['customersPerPageview'] = None
        try:
            output_dict['sessionToContactRate'] = src['sessionToContactRate']
        except:
            output_dict['sessionToContactRate'] = None
        try:
            output_dict['pageviewsPerSession'] = src['pageviewsPerSession']
        except:
            output_dict['pageviewsPerSession'] = None
        try:
            output_dict['opportunities'] = src['opportunities']
        except:
            output_dict['opportunities'] = None
        try:
            output_dict['bounceRate'] = src['bounceRate']
        except:
            output_dict['bounceRate'] = None
        try:
            output_dict['visits'] = src['visits']
        except:
            output_dict['visits'] = None
        try:
            output_dict['visitors'] = src['visitors']
        except:
            output_dict['visitors'] = None
        try:
            output_dict['pageviewsMinusExits'] = src['pageviewsMinusExits']
        except:
            output_dict['pageviewsMinusExits'] = None
        try:
            output_dict['leads'] = src['leads']
        except:
            output_dict['leads'] = None
        try:
            output_dict['leadsPerView'] = src['leadsPerView']
        except:
            output_dict['leadsPerView'] = None
        try:
            output_dict['customers'] = src['customers']
        except:
            output_dict['customers'] = None
        try:
            output_dict['bounces'] = src['bounces']
        except:
            output_dict['bounces'] = None
        try:
            output_dict['time'] = src['time']
        except:
            output_dict['time'] = None
        try:
            output_dict['timePerSession'] = src['timePerSession']
        except:
            output_dict['timePerSession'] = None
        try:
            output_dict['contacts'] = src['contacts']
        except:
            output_dict['contacts'] = None
        try:
            output_dict['newVisitorSessionRate'] = src['newVisitorSessionRate']
        except:
            output_dict['newVisitorSessionRate'] = None
        
        return output_dict
    
    @staticmethod
    def get_insert_stmt(record:dict):
        payload = Traffic_Source_L3.map(record)
        table = Traffic_Source_L3.getTable()
        stmt = table.insert().values(payload)
        return stmt
        
    @staticmethod
    def get_delete_stmt(record:dict):
        payload = Traffic_Source_L3.map(record)
        table = Traffic_Source_L3.getTable()
        stmt = table.delete().where(table.c.id==payload['id'])
        return stmt
        
    def import_to_sql(self,record:dict):
        with self.engine.begin() as connx:
            deleteStmt = Traffic_Source_L3.get_delete_stmt(record)
            insertStmt = Traffic_Source_L3.get_insert_stmt(record)
            connx.execute(deleteStmt)
            connx.execute(insertStmt)


def get_level2_data_labels(date:str,sql_engine:sqlalchemy.engine.Engine):
    query = f"""select parent_breakdown,breakdown from powerbi_python.hubsot_traffic_sources_l2 where date = '{date}'"""
    df = pd.read_sql(query,con=sql_engine)
    label_list = []
    for index,row in df.iterrows():
        temp_dict = {'breakdown':row['breakdown'],"parent_breakdown":row['parent_breakdown']}
        label_list.append(temp_dict)
    return label_list