from dotenv import dotenv_values
from MongoDBAtlas import *
from MySQLDB4Free import *

if __name__ == '__main__':
    config = dotenv_values(".env")
    
    # Consultas utilizando o MongoDB Atlas
    print("\n")
    print("Consultas utilizando o MongoDB Atlas")
    print("\n")
    taskMongo = MongoDBAtlas()
    clientMongo = taskMongo.CreateMongoClient(config['URI'])
    if clientMongo is not None:
        collectionLocalizacaoTransporte = taskMongo.GetCollectionFromDB(clientMongo, "LocalizacaoTransporte")
        # taskMongo.FindOnCollection(collectionLocalizacaoTransporte)
        collectionPesquisaSatisfacaoCliente = taskMongo.GetCollectionFromDB(clientMongo, "PesquisaSatisfacaoCliente")
        # taskMongo.FindOnCollection(collectionPesquisaSatisfacaoCliente,True)
        taskMongo.Question1(collectionLocalizacaoTransporte, "Viação Bonança", "São Vicente / Bandeirantes", "2023-10-18T01:39:37.316Z")
        taskMongo.Question2(collectionLocalizacaoTransporte)
        taskMongo.Question3(collectionLocalizacaoTransporte)
        taskMongo.Question4(collectionLocalizacaoTransporte)
        taskMongo.Question5(collectionPesquisaSatisfacaoCliente)
    else:
        print("Client do MongoDB Atlas não foi instanciado")
    
    # Consultas utilizando o MySQL DB4Free
    print("\n")
    print("Consultas utilizando o MySQL DB4Free")
    print("\n")
    taskMySQL = MySQLDB4Free()
    clientMySQL = taskMySQL.CreateMySQLClient(config['HOST'], config['PORT'], config['USER'], config['PASSWORD'], config['BD'])
    if clientMySQL is not None:
        taskMySQL.Question1(clientMySQL, "Viação Bonança", "São Vicente / Bandeirantes", "2023-10-18T01:39:37.316Z")
        taskMySQL.Question2(clientMySQL)
        taskMySQL.Question3(clientMySQL)
        taskMySQL.Question4(clientMySQL)
        taskMySQL.Question5(clientMySQL)
    else:
        print("Client do MySQLDB4Free não foi instanciado")