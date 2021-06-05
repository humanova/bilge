# (c) 2021 Emir Erbasan (humanova)
#  Bilge : NLP analysis for mergen posts

import confparser
from bilge import Bilge

if __name__ == "__main__":
    config = confparser.get('../config.json')
    redis_config = {'host': config.redis_host, 'port': config.redis_port, 'db': config.redis_db}

    bilge = Bilge(redis_config)
    bilge.start_listening()
