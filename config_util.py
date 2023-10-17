import configparser
import os

SOURCE = "source"
TARGET = "target"
URL = "url"
TOPIC = "topic"
GROUP = "group"
OFFSET = "offset"

def parse_ini(ini_file):
    config = configparser.ConfigParser()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config.read(os.path.join(current_dir, ini_file))

    source = {
        URL: config.get(SOURCE, URL),
        TOPIC: config.get(SOURCE, TOPIC),
        GROUP: config.get(SOURCE, GROUP),
        OFFSET: config.get(SOURCE, OFFSET),
    }
    target = {
        URL: config.get(TARGET, URL),
        TOPIC: config.get(TARGET, TOPIC),
    }

    print("config, source: ", source)
    print("config, target: ", target)
    return source, target
