from elasticsearch import helpers
import json

from esClient import EsClient

#
# 역할 : elasticsearch bulk
#

class EsBulk(EsClient):

    def __init__(self, targetIndex, targetFile):

        EsClient.__init__(self)
        self.filePath = targetFile
        self.targetIndex = targetIndex

    """ bulk 로 document 를 index 합니다.
    """
    def ndJsonDocInsert(self):

        NDJSON_ROW_SIZE = 5

        try:

            f = open(self.filePath, "r", encoding="utf-8")
        except FileNotFoundError as error:
            print(error)
            return
        else:
            ndJsonFile = f.readlines()
            f.close()

            if ndJsonFile:
                for i in range(0, len(ndJsonFile), NDJSON_ROW_SIZE):

                    actions = [ {
                                    "_index" : self.targetIndex,
                                    "_source": json.loads(i)
                                } for i in ndJsonFile[i:i+NDJSON_ROW_SIZE] ]

                    try:

                        helpers.bulk(client=self.es,actions=actions)
                    except:
                        print("bulk insert error")
                        pass

                    print (actions)