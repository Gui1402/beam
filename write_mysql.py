
import mysql.connector
import sqlalchemy
import pandas as pd

database_username = 'root'
database_password = 'Gui@140294'
database_ip       = '127.0.0.1'
database_name     = 'iris'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password,
                                                      database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()

iris = pd.read_csv('https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv')
iris.to_sql(con=database_connection, name='iris_v1', if_exists='append', chunksize=100)
database_connection.close()