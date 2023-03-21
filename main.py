import pandas as pd
from dataio import PostgresDB,JsonData
from datetime import datetime,timedelta
import utils as ut

sql=PostgresDB()

def data_prepare():
    json_data = JsonData('data\Hourly')
    json_data.read_json_files()
    data=json_data.read_json_files()
    data['dsem'] = data['dsem'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    data['nmun'] = data['nmun'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    data['nes'] = data['nmun'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    data['nmun'] = data['nmun'].str.lower().str.title()
    data['dsem'] = data['dsem'].str.lower().str.title()
    data['nes'] = data['dsem'].str.lower().str.title()
    data['dsem'] = data['dsem'].replace({'Miarcoles': 'Miercoles'})
    data['hloc'] = pd.to_datetime(data['hloc'], format='%Y%m%dT%H')
    
    return data


def data_agroup():
    data_2 = data_prepare()
    data_2['hloc'] = pd.to_datetime(data_2['hloc']) # convertir columna a tipo datetime si no lo está
    ultimo_clima = data_2.loc[data_2.groupby('nmun')['hloc'].idxmax()]
    return ultimo_clima
    
def createtable():
    
    data=data_agroup()
    sql.create_table(ut.tablename,data)
    
def insert_tb():
    data=data_agroup()
    sql.insert_data(ut.tablename,data)
        
def data_out(df):
    df=data_agroup()
    dfmunicipios=pd.read_csv('data_municipios\data.csv',delimiter=',',encoding='utf8')
    promedios = df.groupby(['nmun', 'idmun']).agg({'temp': 'mean', 'pre': 'mean'})
    result = pd.merge(promedios[['temp','prec']],dfmunicipios , left_on='idmun', right_on='Cve_Mun', how='left')
    ahora = datetime.now()
    filename = ahora.strftime('%d_%m_%Y_%H_%M')
    
    result.to_csv('data\output\{filename}.csv',index=False)

if __name__ == '__main__':
    data_prepare()
    data_agroup() 
    
    if sql.ejecutar_query('IF EXISTS {TABLE_NAME}'.format):
        createtable()
        insert_tb()
        
    else:
        insert_tb()
    

    data_out()       
    
        



