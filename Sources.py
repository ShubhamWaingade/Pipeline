import findspark

findspark.init()
# import pandas as pd
from pyspark.sql import SparkSession
import configparser

spark = SparkSession.builder.master("local").appName("PySpark_Maria").getOrCreate()


def read_csv(path, schema):
    try:
        df1 = spark.read.csv(path=path, header=True)
        return df1
    except Exception as e:
        return e


def read_data_from_jdbc(database, user, password, port, table, schema):
    try:
        sql = f"select * from sys.{table}"
        database = database
        user = user
        password = password
        server = "localhost"
        port = port
        jdbc_url = f"jdbc:mysql://{server}:{port}/{database}"
        jdbc_driver = 'com.mysql.jdbc.Driver'
        dataframe = spark.read.format("jdbc").options(url=jdbc_url,
                                                      driver=jdbc_driver,
                                                      dbtable=table,
                                                      user=user,
                                                      password=password).load()

        return dataframe
    except Exception as e:
        print(e)


