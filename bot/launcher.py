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


def setup_logging():
    logging.getLogger('discord').setLevel(logging.INFO)
    logging.getLogger('db').setLevel(logging.DEBUG)
    logging.getLogger('quiadit').setLevel(logging.DEBUG)
    logging.getLogger('scrapper').setLevel(logging.DEBUG)
    logging.getLogger('launcher').setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    dt_fmt = '%Y-%m-%d %H:%M:%S'
    fmt = logging.Formatter('[{asctime}] [{levelname:<7}] {name}: {message}', dt_fmt, style='{')

    # Debug rotating logging
    debug_handler = logging.handlers.TimedRotatingFileHandler(when="midnight",
                                                              backupCount=30,
                                                              interval=1,
                                                              filename='../logs/staticord.log',
                                                              encoding='utf-8')
    debug_handler.setFormatter(fmt)
    logger.addHandler(debug_handler)

    # Error logging
    error_handler = logging.FileHandler('../logs/error.log')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(fmt)
    logger.addHandler(error_handler)


def run_bot():
    """
    Load config, connect to database, initialize and launch bot
    """
    loop = asyncio.get_event_loop()
    logger = logging.getLogger(__name__)

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
    setup_logging()
    run_bot()
