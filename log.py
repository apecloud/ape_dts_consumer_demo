import logging
import os

_default_logger = logging.getLogger('default_logger')
_default_logger.setLevel(logging.DEBUG)

_position_logger = logging.getLogger('position_logger')
_position_logger.setLevel(logging.DEBUG)

_commit_logger = logging.getLogger('commit_logger')
_commit_logger.setLevel(logging.DEBUG)

def init(log_dir):
    os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    default_handler = logging.FileHandler(get_log_file(log_dir, "default"), mode='a')
    default_handler.setFormatter(formatter)
    _default_logger.addHandler(default_handler)

    position_handler = logging.FileHandler(get_log_file(log_dir, "position"), mode='a')
    position_handler.setFormatter(formatter)
    _position_logger.addHandler(position_handler)

    commit_handler = logging.FileHandler(get_log_file(log_dir, "commit"), mode='a')
    commit_handler.setFormatter(formatter)
    _commit_logger.addHandler(commit_handler)

def log(msg, level="info"):
    match level:
        case "info":
            return _default_logger.info(msg)
        case "warn":
            return _default_logger.warn(msg)
        case "error":
            return _default_logger.error(msg)
        case "debug":
            return _default_logger.debug(msg)
    return _default_logger.info(msg)

def log_position(msg):
    _position_logger.info(msg)

def log_commit(msg):
    _commit_logger.info(msg)

def get_log_file(log_dir, log_type):
    match log_type:
        case "position":
            return os.path.join(log_dir, 'position.log')
        case "commit":
            return os.path.join(log_dir, 'commit.log')
    return os.path.join(log_dir, 'default.log')