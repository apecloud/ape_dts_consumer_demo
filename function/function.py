import log

def handle(record, config):
    if config.get("print_record") != None and bool(config.get("print_record")):
        log.log("received record: " + str(record))
        log.log("received record: " + str(record), "debug")
        log.log_position("current position: " + str(record))
        log.log_commit("commited record: " + str(record))
