from elasticsearch import Elasticsearch

es_client = Elasticsearch(hosts=['http://localhost:9200'])

def handle(record, config):
    if record["schema"] != "store" or record["tb"] != "book":
        return None
    
    index = "book"
    match record["operation"]:
        case "insert":
            # create a doc
            id = record["after"]["id"]
            doc = {
                'id': record["after"]["id"], 
                'title': record["after"]["title"],
                'author': record["after"]["author"]
            }
            es_client.index(index=index, id=id, body=doc)

        case "update":
            # update a doc
            id = record["before"]["id"]
            doc = {"doc": {
                'title': record["after"]["title"],
                'author': record["after"]["author"]
                }
            }
            es_client.update(index=index, id=id, body=doc)

        case "delete":
            # delete a doc
            id = record["before"]["id"]
            es_client.delete(index=index, id=id)

        case _:
            return None
