from Transformation import dataframe_transformations
import findspark
findspark.init()
import configparser
import logging


def getconfig(database):
    # Config
    global db, user, server, driver, port, password
    config = configparser.ConfigParser(interpolation=None)
    config.read('config/connection_properties')
    db = config[database]['database']
    user = config[database]['user']
    server = config[database]['server']
    driver = config[database]['jdbc_driver']
    port = config[database]['port']
    password = config[database]['password']



try:
    #
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    #
    #
    database = 'mysql'
    config = configparser.ConfigParser()
    getconfig(database)
    df = dataframe_transformations()
    df.show()

    # df.write.format("jdbc").options(
    #     url=f"jdbc:mysql://{server}:{port}/{db}",
    #     driver=driver,
    #     dbtable='NewTable3',
    #     user=user,
    #     password=password).mode('overwrite').save()
    df.write.format("jdbc").options(
        url="jdbc:mysql://localhost/db",
        driver='com.mysql.jdbc.Driver',
        dbtable='NewTable3',
        user='root',
        password='12345').mode('append').save()

    logger.info("Data sent to MySql Database")
except Exception as e:
    logger .info(e)