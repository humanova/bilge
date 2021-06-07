# (c) 2021 Emir Erbasan (humanova)
#  Bilge : NLP analysis for mergen posts

import confparser
from bilge import Bilge
import traceback

from logger import logging

if __name__ == "__main__":
    config = confparser.get('../config.json')
    redis_config = {'host': config.redis_host, 'port': config.redis_port, 'db': config.redis_db}

    try:
        bilge = Bilge(redis_config)
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logging.fatal(f"[Bilge] Couldn't initialize bilge : {e}")
        quit()
    bilge.start_listening()
