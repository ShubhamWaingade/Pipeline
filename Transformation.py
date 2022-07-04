import findspark

findspark.init()
from pyspark.sql import *
import logging
from Sources import read_csv, read_data_from_jdbc

spark = SparkSession.builder.master("local").appName("PySpark_Maria").getOrCreate()


def dataframe_transformations():
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    try:
        schema = spark.read.json("C:/Users/ShubhamWaingade/Desktop/VJ/DataWarehouse/Pipeline/dataframe_schema.txt")
        # df = read_csv("C:/Users/ShubhamWaingade/Test Folder/Data/regions.csv")
        df2 = read_data_from_jdbc(database='db',
                                  user='root',
                                  password='12345',
                                  port='3306',
                                  table="NewTable3",
                                  schema=schema)

        df2 = df2.drop(df2.character)
        df2 = df2[(df2.role == "ACTOR") & (df2.id == "tm84618")]
        df2 = df2.dropDuplicates()
        logger.info("Data import done")
        return df2
        logger.info("Data Transformation done!")
    except Exception as e:
        logger.info(e)
        logger.info("Error while loading data, please refer to Transformations.py")


df = dataframe_transformations()
df.show()