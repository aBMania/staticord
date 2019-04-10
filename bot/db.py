"""
Database interaction module
"""
import asyncio
import datetime
import logging
from typing import Optional

import asyncpg
import discord

INSERT_MESSAGE_SQL = """
INSERT INTO staticord.message (id, guild, channel, user_id, content, datetime)
    VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (id) DO UPDATE
    SET guild = message.guild,
        channel = message.channel, 
        user_id = message.user_id,
        content = message.content,
        datetime = message.datetime;
"""

INSERT_MEMBER_SQL = """
INSERT INTO staticord.member (id, guild, name)
    VALUES ($1, $2, $3)
ON CONFLICT (id, guild) DO UPDATE
    SET name = member.name
;
"""

INSERT_GUILD_SQL = """
INSERT INTO staticord.guild (id, name)
    VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE
    SET name = guild.name;
"""

INSERT_ACTIVITY_SQL = """
INSERT INTO staticord.activity (
    guild, 
    member, 
    status, 
    type, 
    name, 
    start,
    "end",
    listening_title,
    listening_artist,
    listening_album,
    listening_track_id,
    listening_party
) 
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
"""

INSERT_NICKNAME_SQL = """
INSERT INTO staticord.nickname (guild, member, nickname, datetime) 
    VALUES ($1, $2, $3, $4)
"""

MEMBER_LAST_NICKNAME_SQL = """
SELECT nickname FROM staticord.nickname
    WHERE guild = $1
        AND member = $2
ORDER BY datetime DESC
LIMIT 1
"""

MEMBER_LAST_ACTIVITY_SQL = """
SELECT     
    guild, 
    member, 
    status, 
    type, 
    name, 
    start,
    "end",
    listening_title,
    listening_artist,
    listening_album,
    listening_track_id,
    listening_party
    FROM staticord.activity
    WHERE guild = $1
        AND member = $2
ORDER BY start DESC
LIMIT 1
"""

CHANNEL_LAST_MESSAGE_DATE_SQL = """
SELECT max(datetime) FROM staticord.message
    WHERE channel = $1
"""

GUILD_MEMBER_EMOJIS = """
SELECT member, emoji
FROM staticord.member_emoji
WHERE guild = $1;
"""

RANDOM_MESSAGES = """
SELECT
  msg.content,
  m.name,
  m.id,
  me.emoji
FROM staticord.message msg
JOIN staticord.member m on m.id = msg.user_id and m.guild = msg.guild
JOIN staticord.member_emoji me on me.member = m.id
WHERE m.guild = $1
   AND length(msg.content) > $2
ORDER BY random()
LIMIT $3;
"""

logger = logging.getLogger(__name__)


class Db:
    """Database interacetion class"""

    def __init__(self, config):
        self.pool: asyncpg.pool.Pool = None
        self.config = config

    async def connect_db(self) -> None:
        """Connect to db"""

        logger.info('Connecting to postgres host: %s, user: %s',
                    self.config['db']['host'],
                    self.config['db']['user'])
        n_try = 0
        n_max_try = 3
        wait_time = 1  # in seconds

        while not self.pool and n_try < n_max_try:
            n_try += 1

            try:
                self.pool = await asyncpg.create_pool(host=self.config['db']['host'],
                                                      user=self.config['db']['user'],
                                                      password=self.config['db']['password'])
            except asyncpg.PostgresError as err:
                logger.error('Cannot connect to database, retry in %d seconds', wait_time)
                await asyncio.sleep(wait_time)

                if n_try < n_max_try:
                    break
                else:
                    logger.error('Max number of connection tries attempted, exiting')
                    raise err

        logger.debug('Connected to database')

    async def save_mesage(self, message: discord.Message) -> None:
        """Save message to db"""

        logger.debug('Save message %s', message)

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(INSERT_MESSAGE_SQL, message.id, message.guild.id,
                                   message.channel.id, message.author.id,
                                   message.content, message.created_at)

        except asyncpg.PostgresError as err:
            logger.error('PostgresError %s, message that created exception %s', err, message)

    async def save_member_activity(self, member: discord.Member) -> None:
        """Save member activity to db"""

        logger.debug('Saving activity %s for member %s', member.activity, member.name)

        activity_type = member.activity.type.name if member.activity else None
        activity_name = member.activity.name if member.activity else None
        start = member.activity.start if member.activity else datetime.datetime.now()
        end = member.activity.end if member.activity else None

        listening_title = member.activity.title if isinstance(member.activity, discord.Spotify) \
            else None
        listening_artist = member.activity.artist if isinstance(member.activity, discord.Spotify) \
            else None
        listening_album = member.activity.album if isinstance(member.activity, discord.Spotify) \
            else None
        listening_track_id = member.activity.track_id if isinstance(member.activity,
                                                                    discord.Spotify) \
            else None
        listening_party = member.activity.party_id if isinstance(member.activity, discord.Spotify) \
            else None

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(INSERT_ACTIVITY_SQL, member.guild.id, member.id,
                                   member.status.value, activity_type, activity_name, start,
                                   end, listening_title, listening_artist, listening_album,
                                   listening_track_id, listening_party)

        except asyncpg.PostgresError as err:
            logger.error('PostgresError %s, activity provocing %s, from '
                         'member: %s', err, member.activity, member)

    async def get_member_last_nickname(self, member: discord.Member) -> Optional[str]:
        """Get the last nickname of a member"""

        logger.debug('Get member last nickname %s', member)

        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchval(MEMBER_LAST_NICKNAME_SQL, member.guild.id, member.id) \
                       or None
        except asyncpg.PostgresError as err:
            logger.error('PostgresError %s, while fetching nickname of '
                         'member: %s', err, member)

    async def get_member_last_activity(self, member: discord.Member) -> Optional:
        """Get the last nickname of a member"""

        logger.debug('Get member last activity %s', member)

        try:
            async with self.pool.acquire() as conn:
                activity_row = await conn.fetchrow(MEMBER_LAST_ACTIVITY_SQL, member.guild.id,
                                                   member.id)
            return {
                'guild': activity_row['guild'],
                'member': activity_row['member'],
                'status': activity_row['status'],
                'type': activity_row['type'],
                'name': activity_row['name'],
                'start': activity_row['start'],
                'end': activity_row['end'],
                'listening_title': activity_row['listening_title'],
                'listening_artist': activity_row['listening_artist'],
                'listening_album': activity_row['listening_album'],
                'listening_track_id': activity_row['listening_track_id'],
                'listening_party': activity_row['listening_party']
            } if activity_row else None

        except asyncpg.PostgresError as err:
            logger.error('PostgresError %s, while fetching activity of member: %s', err, member)

    async def save_member_nickname(self, member: discord.Member) -> None:
        """Save member nickname to db if it has changed"""

        logger.debug('Saving nickname %s', member.nick)

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(INSERT_NICKNAME_SQL, member.guild.id, member.id, member.nick,
                                   datetime.datetime.now())

        except asyncpg.PostgresError as err:
            logger.error('PostgresError %s, activity provocing %s, from member: %s', err,
                         member.activity, member)

    async def get_channel_last_saved_message_date(self, channel: discord.TextChannel) -> Optional[
            datetime.datetime]:
        """Get the timestamp of the last message of a channel"""

        logger.debug('Get channel last message %s', channel)

        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchval(CHANNEL_LAST_MESSAGE_DATE_SQL, channel.id) or None

        except asyncpg.PostgresError as err:
            logger.error('PostgresError %s, get last from channel %s', err, channel.id)

    async def save_member(self, member: discord.Member):
        """Save member nickname to db if it has changed"""

        logger.debug('Saving member %s', member.name)

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(INSERT_MEMBER_SQL, member.id, member.guild.id, member.name)

        except asyncpg.PostgresError as err:
            logger.error('PostgresError %s, member provocing %s', err, member)

    async def save_guild(self, guild: discord.Guild):
        """Save member nickname to db if it has changed"""

        logger.debug('Saving guild %s', guild.name)

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(INSERT_GUILD_SQL, guild.id, guild.name)

        except asyncpg.PostgresError as err:
            logger.error('PostgresError %s, guild provocing %s', err, guild)

    async def get_member_emojis(self, guild: discord.Guild):
        """Get emojis tied to members"""

        logger.debug('Get member emojis of guild %s', guild.name)

        try:
            async with self.pool.acquire() as conn:
                records = await conn.fetch(GUILD_MEMBER_EMOJIS, guild.id)
                return [dict(r) for r in records]

        except asyncpg.PostgresError as err:
            logger.error('PostgresError %s, guild provocing %s', err, guild)

    async def get_random_messages(self, guild: discord.Guild, n_messages: int):
        """Get emojis tied to members"""

        logger.debug('Get member emojis of guild %s', guild.name)

        try:
            async with self.pool.acquire() as conn:
                records = await conn.fetch(RANDOM_MESSAGES, guild.id, 50, n_messages)
                return [dict(r) for r in records]

        except asyncpg.PostgresError as err:
            logger.error('PostgresError %s, guild provocing %s', err, guild)
