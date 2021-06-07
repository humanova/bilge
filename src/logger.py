import logging

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    level=logging.INFO,

    handlers=[
        logging.FileHandler("../bilge.log"),
        logging.StreamHandler()],
    datefmt='%Y-%m-%d %H:%M:%S')