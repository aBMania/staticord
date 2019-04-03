"""
Launcher module
Entry point of the bot
"""

import logging
import logging.handlers


import asyncio
import asyncpg
import yaml
from discord.ext.commands import Bot

from db import Db
from quiadit import QuiADit
from scrapper import Scrapper

logging.getLogger('discord').setLevel(logging.INFO)
logging.getLogger('db').setLevel(logging.DEBUG)
logging.getLogger('quiadit').setLevel(logging.DEBUG)
logging.getLogger('scrapper').setLevel(logging.DEBUG)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

handler = logging.handlers.TimedRotatingFileHandler(when="midnight",
                                                    backupCount=30,
                                                    interval=1,
                                                    filename='../logs/staticord.log',
                                                    encoding='utf-8')
dt_fmt = '%Y-%m-%d %H:%M:%S'
fmt = logging.Formatter('[{asctime}] [{levelname:<7}] {name}: {message}', dt_fmt, style='{')
handler.setFormatter(fmt)

root_logger.addHandler(handler)


def run_bot():
    """
    Load config, connect to database, initialize and launch bot
    """
    loop = asyncio.get_event_loop()
    logger = logging.getLogger()

    with open('../config/config.yml', 'r') as config_file:
        config = yaml.load(config_file, Loader=yaml.BaseLoader)

    try:
        db = Db(config)
        loop.run_until_complete(db.connect_db())
    except asyncpg.PostgresError as err:
        logger.error('Cannot connect to database: %s', err)
        return

    bot = Bot(command_prefix=config['bot']['prefix'])
    bot.db = db
    bot.add_cog(Scrapper(bot))
    bot.add_cog(QuiADit(bot))
    bot.run(config['bot']['token'])


if __name__ == '__main__':
    run_bot()
