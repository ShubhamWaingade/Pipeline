import configparser
import logging


# if __name__ == '__main__':
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

print(db)