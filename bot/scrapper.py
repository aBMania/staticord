"""
Scrapper
Will watch and log channels and users activities
"""
import logging

from discord.ext import commands

import discord

REFRESH_RATE = 60  # seconds

logger = logging.getLogger(__name__)


class Scrapper(commands.Cog):
    """Poll voting system."""

    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        """Log when ready"""
        logger.info('Logged in as %s (id = %s)', self.bot.user.name, self.bot.user.id)
        await self.save_guilds_data()

    @commands.Cog.listener()
    async def on_message(self, message):
        """Called on message"""
        logger.info('Save received message %s', message)
        await self.bot.db.save_mesage(message)

    @commands.Cog.listener()
    async def on_member_join(self, member):
        """Called on member join"""
        logger.info('Save new member %s', member)
        await self.save_member(member)

    @commands.Cog.listener()
    async def on_member_update(self, _, member):
        """Called on member update"""
        logger.info('Save updated member %s', member)
        await self.save_member(member)

    @commands.Cog.listener()
    async def on_guild_update(self, _, guild):
        """Called on guild update"""
        logger.info('Save updated guild data %s', guild)
        await self.save_guild_data(guild)

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """Called on guild join"""
        logger.info('Save joined guild data %s', guild)
        await self.save_guild_data(guild)

    async def save_guilds_data(self) -> None:
        """Fetch data (message + members) from all guilds"""

        for guild in self.bot.guilds:
            await self.save_guild_data(guild)

    async def save_guild_data(self, guild: discord.Guild) -> None:
        """Fetch data (messages + members) from one guild"""

        logger.info('Fetching members, message and activities from channels of guild %s',
                    guild.name)

        await self.bot.db.save_guild(guild)
        await self.save_members(guild)
        await self.save_channels_messages(guild)

    async def save_channels_messages(self, guild: discord.Guild) -> None:
        """Fetch messages from all channels of a guild"""

        for channel in guild.text_channels:
            await self.save_text_channel_data(channel)

    async def save_text_channel_data(self, channel: discord.TextChannel) -> None:
        """Fetch messages from channel"""

        permissions = channel.permissions_for(channel.guild.me)
        if permissions.read_message_history:
            logger.info('Fetching message history from channels from guild %s, channel %s',
                        channel.guild.name, channel.name)

            channel_last_message_date = await self.bot.db.get_channel_last_saved_message_date(
                channel)

            async for message in channel.history(limit=None,
                                                 after=channel_last_message_date,
                                                 reverse=True):
                await self.bot.db.save_mesage(message)

    async def save_members(self, guild: discord.Guild):
        """Save all members of a guild"""
        for member in guild.members:
            await self.save_member(member)

    async def save_member(self, member: discord.Member):
        """Save a particular member with its current nickname and activity"""
        if not member.bot:
            await self.bot.db.save_member(member)
            await self.save_member_activity(member)
            await self.save_member_nickname(member)

    async def save_member_activity(self, member: discord.Member):
        """Save member activity if it has changed"""
        last_member_activity = await self.bot.db.get_member_last_activity(member)

        if last_member_activity != member.activity:
            logger.debug('Saving new activity for %s: %s', member.name, member.activity)

            await self.bot.db.save_member_activity(member)

    async def save_member_nickname(self, member: discord.Member):
        """Save member nickname if it has changed"""
        last_member_nickname = await self.bot.db.get_member_last_nickname(member)
        if last_member_nickname != member.nick:
            logger.debug('Saving new nickname for %s: %s', member.name, member.nick)
            await self.bot.db.save_member_nickname(member)
