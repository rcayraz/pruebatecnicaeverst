
import os
URL_METOD_1="https://smn.conagua.gob.mx/es/web-service-api/webservices/?method=1"

LOCAL_DATA_DAILY='data\Daily'
LOCAL_DATA_HOURLY='data\Hourly'
dbname = os.environ.get('dbname', '')
user = os.environ.get('user', '')
password = os.environ.get('password', '')
host = os.environ.get('host', '')
port =os.environ.get('port', '')
tablename=os.environ.get('tablename', '')
path_municipios='data_municipios\data.csv'

