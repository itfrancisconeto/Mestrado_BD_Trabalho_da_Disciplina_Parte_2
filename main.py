from dotenv import dotenv_values
from MongoDB import *
from MySQLDB import *

if __name__ == '__main__':
    
    config = dotenv_values(".env")
    
    # Parametrização das bases    
    taskMongo = MongoDB()
    taskMySQL = MySQLDB()

    # # MongoDB Local
    # clientMongoLocal = taskMongo.CreateMongoClient(config['LocalURI']) 
    # flagMongo = "Local"
    # taskMongo.ConsultasMongoDB(clientMongoLocal, taskMongo, flagMongo)
    
    # MongoDB Atlas
    clientMongoAtlas = taskMongo.CreateMongoClient(config['AtlasURI']) 
    flagMongo = "Atlas"
    taskMongo.ConsultasMongoDB(clientMongoAtlas, taskMongo, flagMongo)
    
    # # MySQL Local
    # clientMySQLLocal = taskMySQL.CreateMySQLClient(config['LocalHOST'], config['LocalUSER'], config['LocalPASSWORD'], config['LocalBD']) 
    # flagMySQLDB = "Local"
    # taskMySQL.ConsultasMySQLDB(clientMySQLLocal, taskMySQL, flagMySQLDB)
    
    # MySQL Lince On Line
    clientMySQLLince= taskMySQL.CreateMySQLClient(config['LinceHOST'], config['LinceUSER'], config['LincePASSWORD'], config['LinceBD']) 
    flagMySQLDB = "Lince On Line"
    taskMySQL.ConsultasMySQLDB(clientMySQLLince, taskMySQL, flagMySQLDB)

    