import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0, password="123456")

def handle(record):
    if record["schema"] != "store" or record["tb"] != "book_stock":
        return None

    match record["operation"]:
        case "insert":
            # set a kv
            book_id = record["after"]["book_id"]
            inventory = record["after"]["inventory"]
            redis_client.set(book_id, inventory)
            
        case "update":
            book_id = record["after"]["book_id"]
            inventory = record["after"]["inventory"]
            redis_client.set(book_id, inventory)
            
        case "delete":
            book_id = record["before"]["book_id"]
            redis_client.delete(book_id)
            
        case _:
            return None
