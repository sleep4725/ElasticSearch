from elasticsearch import Elasticsearch
import requests
import json, yaml

#
# 작성자 : 김준현
#
class EsClient:

    def __init__(self):
        self.es = EsClient.isElasticAlive()

    """ elastic cluster conn 하기 위한 정보가 들어있는 파일을 읽어 온다.
        @return yamlFile 
    """
    @classmethod
    def esConfigGet(cls):
        # elastics connection information get
        try:
            f=open("./config/info.yml", "r", encoding="utf-8")
        except FileNotFoundError as error:
            print(error)
            return
        else:
            infoDoc = yaml.safe_load(f)
            f.close()
            return infoDoc

    """ elastic cluster 서버가 정상적으로 기동되고 있는지 체크
        elastic cluster의 health가 정상적(?)인지 체크 
        @return elasticClient
    """
    @classmethod
    def isElasticAlive(cls):
        #______________________________________________________
        infoDoc = EsClient.esConfigGet()
        sess = requests.Session()
        isGood = False
        try:
            html = sess.get(url=infoDoc.get("esHost"))
        except requests.exceptions.ConnectionError as error:
            print(error)
            pass
        else:
            if html.status_code == 200 and html.ok:
                isGood = True
        finally:
            sess.close()

        if isGood:
            esConn = Elasticsearch(hosts=[infoDoc.get("esHost")])
            response = esConn.cluster.health()
            if response["status"] == "yellow" or response["status"] == "green":
                return esConn
            else:
                print("elastic cluster status red")
                exit(1)
        else:
            exit(1)
