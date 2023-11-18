from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import datetime
import time
import os
from dotenv import load_dotenv, dotenv_values

class MongoDBAtlas(object):

    def __init__(self):
        ...
    
    def CreateMongoClient(self, uri)->MongoClient:
        # Create a new client and connect to the server
        client = MongoClient(uri, server_api=ServerApi('1'))        
        # Send a ping to confirm a successful connection
        try:
            client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
            print("\n")
            return client                       
        except Exception as e:
            print(e)
            return None                    

    def GetCollectionFromDB(self, client, collectionName)->any:
        db = client.trabalho_parte_1
        collection = db[collectionName]
        return collection

    def FindOnCollection(self, collection, isLookup)->None:
        FindOnCollectionStart = time.time()
        if isLookup and collection.name == "PesquisaSatisfacaoCliente":
            result = collection.aggregate([{'$lookup': {'from': 'ClassificacaoNPS', 'localField': 'nota', 'foreignField': 'nota', 'as': 'classificacaoNPS'}}])
        else:
            result = collection.find()        
        if collection.name == "LocalizacaoTransporte" and result:    
            for doc in result:
                _id = doc['_id']
                empresa = doc['empresa']
                linha = doc['linha']
                latitude = doc['latitude']
                longitude = doc['longitude']
                datahora = doc['datahora']
                print(f" id: {_id}\n empresa: {empresa}\n linha: {linha}\n latitude: {latitude}\n longitude: {longitude}\n datahora: {datahora}\n")        
        elif collection.name == "PesquisaSatisfacaoCliente" and result:    
            for doc in result:
                _id = doc['_id']
                empresa = doc['empresa']
                linha = doc['linha']
                nota = doc['nota']
                if isLookup:
                    classificacao = '(' + doc['classificacaoNPS'][0]['classificacao'] + ')' 
                else:
                    classificacao = ""
                datahora = doc['datahora']
                print(f" id: {_id}\n empresa: {empresa}\n linha: {linha}\n nota: {nota} {classificacao}\n datahora: {datahora}\n")         
        else:
            print("No documents found.")
            print("\n")
        FindOnCollectionEnd = time.time()
        print(f"Tempo de busca na colleção: {round(FindOnCollectionEnd - FindOnCollectionStart,3)} segundos")
        print("\n")

    def Question1(self, collection, empresa, linha, datahora)->None:
        print("<<< Questão 1 >>> Qual é a localização do transporte de interesse em tempo real?")
        d = datetime.datetime.strptime(datahora, "%Y-%m-%dT%H:%M:%S.%fZ")
        Question1Start = time.time()
        doc = collection.find_one({"empresa": empresa, "linha": linha, "datahora": d})
        if doc is not None:
            latitude = doc['latitude']
            longitude = doc['longitude']
            datahora = doc['datahora']
            print(f"Latitude: {latitude}\nLongitude: {longitude}\nData/Hora: {datahora}") 
        else:
            print("No documents found.")
        Question1End = time.time()
        print(f"Tempo da Questão 1: {round(Question1End - Question1Start,3)} segundos")
        print("\n")            

    def Question2(self, collection)->None:
        print("<<< Questão 2 >>> Qual linha teve sua localização mais compartilhada?")
        Question2Start = time.time()
        result_cursor = collection.aggregate([{"$group":{"_id":"$linha","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":1}])
        if result_cursor is not None:            
            print(f"A linha que teve sua localização mais compartilhada foi:")
            for doc in result_cursor:
                print(doc)
        else:
            print("No documents found.")
        Question2End = time.time()
        print(f"Tempo da Questão 2: {round(Question2End - Question2Start,2)} segundos")
        print("\n")
    
    def Question3(self, collection)->None:
        print("<<< Questão 3 >>> Qual empresa teve a localização de seus ônibus mais compartilhada?")
        Question3Start = time.time()
        result_cursor = collection.aggregate([{"$group":{"_id":"$empresa","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":1}])
        if result_cursor is not None:            
            print(f"A empresa que teve sua localização mais compartilhada foi:")
            for doc in result_cursor:
                print(doc)
        else:
            print("No documents found.")
        Question3End = time.time()
        print(f"Tempo da Questão 3: {round(Question3End - Question3Start,3)} segundos")
        print("\n")
    
    def Question4(self, collection)->None:
        print("<<< Questão 4 >>> Qual horário concentrou o maior volume de compartilhamentos?")
        Question4Start = time.time()
        result_cursor = collection.aggregate([{"$project":{"hour":{"$hour":"$datahora"}}},{"$group":{"_id":"$hour","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":1}])
        if result_cursor is not None:            
            print(f"O horário que concentrou o maior volume de compartilhamentos foi:")
            for doc in result_cursor:
                print(doc)
        else:
            print("No documents found.")
        Question4End = time.time()
        print(f"Tempo da Questão 4: {round(Question4End - Question4Start,3)} segundos")
        print("\n")
    
    def Question5(self, collection)->None:
        print("<<< Questão 5 >>> Qual empresa obteve o maior total de pontos na pesquisa de satisfação de seus clientes?")
        Question5Start = time.time()
        result_cursor = collection.aggregate([{"$group":{"_id":"$empresa","totalNota":{"$sum":"$nota"}}},{"$sort":{"totalNota":-1}},{"$limit":1}])
        if result_cursor is not None:            
            print(f"A empresa que obteve o maior total de pontos foi:")
            for doc in result_cursor:
                print(doc)
        else:
            print("No documents found.")
        Question5End = time.time()
        print(f"Tempo da Questão 5: {round(Question5End - Question5Start,3)} segundos")
        print("\n")

if __name__ == '__main__':
    config = dotenv_values(".env")
    taskMongo = MongoDBAtlas()
    client = taskMongo.CreateMongoClient(config['URI'])
    if client is not None:
        collectionLocalizacaoTransporte = taskMongo.GetCollectionFromDB(client, "LocalizacaoTransporte")
        # taskMongo.FindOnCollection(collectionLocalizacaoTransporte)
        collectionPesquisaSatisfacaoCliente = taskMongo.GetCollectionFromDB(client, "PesquisaSatisfacaoCliente")
        # taskMongo.FindOnCollection(collectionPesquisaSatisfacaoCliente,True)
        taskMongo.Question1(collectionLocalizacaoTransporte, "Viação Bonança", "São Vicente / Bandeirantes", "2023-10-18T01:39:37.316Z")
        taskMongo.Question2(collectionLocalizacaoTransporte)
        taskMongo.Question3(collectionLocalizacaoTransporte)
        taskMongo.Question4(collectionLocalizacaoTransporte)
        taskMongo.Question5(collectionPesquisaSatisfacaoCliente)
