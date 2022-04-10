import pymysql
from etl import ETL
from es_loader import ESLoader

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='password',
                             database='test',
                             cursorclass=pymysql.cursors.DictCursor)

loader = ESLoader('http://localhost:9200')

etl_vodomat = ETL(connection, loader)

if __name__ == '__main__':
    etl_vodomat.load('avtomats')
