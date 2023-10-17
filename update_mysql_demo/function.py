def handle(record):
    if record["schema"] != "store" or record["tb"] != "book":
        return None
    
    new_field = {
        "name": "merchant",
        "type_name": "string"
    }

    match record["operation"]:
        case "insert":
            # case 1, change values of some fields
            # record["after"]["id"] = record["after"]["id"] + 200000
            # return [record]
        
            # case 2, add some fields
            record["fields"].append(new_field)
            record["after"][new_field["name"]] = "alibaba"
            return [record]
        
            # case 3, to ignore the change, return None
            # return None 
        case "update":
            # case 1, change values of some fields
            # record["after"]["id"] = record["after"]["id"] + 200000
            # return [record]
        
            # case 2, add some fields
            record["fields"].append(new_field)
            record["before"][new_field["name"]] = None
            record["after"][new_field["name"]] = "alibaba"
            return [record]
        
            # case 3, to ignore the change, return None
            # return None 
        case "delete":
            # case 1, change values of some fields
            # record["before"]["id"] = record["before"]["id"] + 200000
            # return [record]

            # case 2, add some fields
            record["fields"].append(new_field)
            record["before"][new_field["name"]] = "alibaba"
            return [record]
        
            # case 3, to ignore the change, return None
            # return None 
        case _:
            return None
