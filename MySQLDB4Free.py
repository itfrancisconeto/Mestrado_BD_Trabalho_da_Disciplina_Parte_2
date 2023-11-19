import mysql
import mysql.connector
import datetime
import time

class MySQLDB4Free(object):

    def __init__(self):
        ...
    
    def CreateMySQLClient(self, _host, _port, _user, _password, _bd)->mysql:
        # Create a new client and connect to the server
        client = mysql.connector.connect(
            host=_host,
            port=_port,
            user=_user,
            password=_password,
            database=_bd
            )        
        # Send a ping to confirm a successful connection
        try:
            if (client.is_connected()):
                print("You successfully connected to MySQL!")
            else:
                print("Not connected to MySQL")
                print("\n")
            return client                       
        except Exception as e:
            print(e)
            return None                    

    def Question1(self, client, empresa, linha, datahora)->None:
        print("<<< Questão 1 >>> Qual é a localização do transporte de interesse em tempo real?")
        d = datetime.datetime.strptime(datahora, "%Y-%m-%dT%H:%M:%S.%fZ")
        Question1Start = time.time()
        cursor = client.cursor()
        cursor.execute("""select e.Nome, l.Nome, p.Latitude, p.Longitude, p.DataHora from Posicao p 
                        inner join Empresa e on p.IdEmpresa  = e.IdEmpresa 
                        inner join Linha l on p.IdLinha  = l.IdLinha 
                        where 
                        e.Nome = 'Viação Bonança' and 
                        l.Nome = 'São Vicente / Bandeirantes' and 
                        p.DataHora = '2023-10-18 01:39:37';
                       """)
        result = cursor.fetchall()
        for value in result:
            print(value) 
        Question1End = time.time()
        print(f"Tempo da Questão 1: {round(Question1End - Question1Start,3)} segundos")
        print("\n")              

    def Question2(self, client)->None:
        print("<<< Questão 2 >>> Qual linha teve sua localização mais compartilhada?")
        Question2Start = time.time()
        cursor = client.cursor()
        cursor.execute("""select l.Nome, count(p.IdLinha) from Posicao p 
                        inner join Linha l on p.IdLinha  = l.IdLinha
                        group by 1
                        order by 2 desc
                        limit 1;
                       """)
        result = cursor.fetchall()
        for value in result:
            print(value) 
        Question2End = time.time()
        print(f"Tempo da Questão 2: {round(Question2End - Question2Start,2)} segundos")
        print("\n")
    
    def Question3(self, client)->None:
        print("<<< Questão 3 >>> Qual empresa teve a localização de seus ônibus mais compartilhada?")
        Question3Start = time.time()
        cursor = client.cursor()
        cursor.execute("""select e.Nome, count(p.IdEmpresa) from Posicao p 
                        inner join Empresa e on p.IdEmpresa = e.IdEmpresa 
                        group by 1
                        order by 2 desc
                        limit 1;
                        """)
        result = cursor.fetchall()
        for value in result:
            print(value) 
        Question3End = time.time()
        print(f"Tempo da Questão 3: {round(Question3End - Question3Start,3)} segundos")
        print("\n")
    
    def Question4(self, client)->None:
        print("<<< Questão 4 >>> Qual horário concentrou o maior volume de compartilhamentos?")
        Question4Start = time.time()
        cursor = client.cursor()
        cursor.execute("""select hour(DataHora), count(hour(DataHora)) from Posicao p
                            group by 1
                            order by 2 desc
                            limit 1;
                        """)
        result = cursor.fetchall()
        for value in result:
            print(value) 
        Question4End = time.time()
        print(f"Tempo da Questão 4: {round(Question4End - Question4Start,3)} segundos")
        print("\n")
    
    def Question5(self, client)->None:
        print("<<< Questão 5 >>> Qual empresa obteve o maior total de pontos na pesquisa de satisfação de seus clientes?")
        Question5Start = time.time()
        cursor = client.cursor()
        cursor.execute("""select e.Nome, sum(ps.Nota) from PesquisaSatisfacao ps
                            inner join Empresa e on ps.IdEmpresa = e.IdEmpresa 
                            group by 1
                            order by 2 desc
                            limit 1;
                        """)
        result = cursor.fetchall()
        for value in result:
            print(value) 
        self.DisconnectMySQLClient(client, cursor)
        Question5End = time.time()
        print(f"Tempo da Questão 5: {round(Question5End - Question5Start,3)} segundos")
        print("\n")

    def DisconnectMySQLClient (self, client, cursor):
        cursor.close()
        client.close()