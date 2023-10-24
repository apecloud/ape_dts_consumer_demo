def handle(record, config):
    if config.get("print_record") != None and bool(config.get("print_record")):
        print("received record: ", record)
