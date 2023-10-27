import configparser
import os

EXTRACTOR = "extractor"
SINKER = "sinker"
CUSTOM = "custom"

def parse_ini(ini_file):
    config = configparser.ConfigParser()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config.read(os.path.join(current_dir, ini_file))

    extractor = parse_section(config, EXTRACTOR)
    sinker = parse_section(config, SINKER)
    custom = parse_section(config, CUSTOM)
    return extractor, sinker, custom

def parse_section(config, section):
    kvs = {}
    if not config.has_section(section):
        return kvs
    for k in config.options(section):
        v = config.get(section, k)
        if v != None and v != "":
            kvs[k] = v
    return kvs