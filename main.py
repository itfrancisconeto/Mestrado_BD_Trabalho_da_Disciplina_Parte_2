from dotenv import dotenv_values
from MongoDB import *
from MySQLDB import *

if __name__ == '__main__':
    
    config = dotenv_values(".env")
    
    # Parametrização das bases
    taskMongo = MongoDB()
    # MongoDB Local
    clientMongoLocal = taskMongo.CreateMongoClient(config['LocalURI']) 
    flagMongo = "Local"
    taskMongo.ConsultasMongoDB(clientMongoLocal, taskMongo, flagMongo)
    # MongoDB Atlas
    clientMongoAtlas = taskMongo.CreateMongoClient(config['AtlasURI']) 
    flagMongo = "Atlas"
    taskMongo.ConsultasMongoDB(clientMongoAtlas, taskMongo, flagMongo)
    
    taskMySQL = MySQLDB()
    # MySQL Local
    clientMySQLLocal = taskMySQL.CreateMySQLClient(config['LocalHOST'], config['LocalPORT'], config['LocalUSER'], config['LocalPASSWORD'], config['LocalBD']) 
    flagMySQLDB = "Local"
    taskMySQL.ConsultasMySQLDB(clientMySQLLocal, taskMySQL, flagMySQLDB)
    # MySQL DB4Free
    clientMySQLDB4free = taskMySQL.CreateMySQLClient(config['DB4freeHOST'], config['DB4freePORT'], config['DB4freeUSER'], config['DB4freePASSWORD'], config['DB4freeBD']) 
    flagMySQLDB = "DB4free"
    taskMySQL.ConsultasMySQLDB(clientMySQLDB4free, taskMySQL, flagMySQLDB)

    