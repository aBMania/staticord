"""
QuiADit cog
This is a game to guess who said a message chosen randomly in the database
"""

import logging
import asyncio

import discord
from discord.ext import commands

logger = logging.getLogger(__name__)


class QuiADit(commands.Cog):
    """Poll voting system."""

    def __init__(self, bot):
        self.bot = bot

    @commands.command()
    @commands.guild_only()
    async def quiadit(self, ctx):
        """
        Launch QuiADit game

        TODO: refactor
        """

        member_emojis = await self.bot.db.get_member_emojis(ctx.guild)
        all_emojis = ctx.guild.emojis

        for member_emoji in member_emojis:
            emoji_of_member = [e for e in all_emojis if e.name == member_emoji['emoji']]
            emoji_of_member = emoji_of_member[0] if emoji_of_member else member_emoji['emoji']
            member_emoji['object'] = emoji_of_member

        # a list of messages to delete when we're all done
        questions = await self.bot.db.get_random_messages(ctx.guild, 40)
        i = 0
        for q in questions:
            i = i + 1
            question = q['content'].replace("@","@ ")
            answer = q['emoji']
            answer_name = q['name']

            question_message: discord.Message = await ctx.send(
                f'**Qui a dit ? ({i}/{len(questions)})**'
                f'\n**-------------------------------------------**'
                f'\n'
                f'\n{question}'
                f'\n'
                f'\n**-------------------------------------------**')

            for emoji in member_emojis:
                try:
                    self.bot.loop.create_task(await question_message.add_reaction(emoji['object']))
                except discord.DiscordException as err:
                    logger.error('Error while adding reaction %s: %s', emoji['object'], err)

            losers = []

            check = lambda _reaction, _user, message=question_message, _losers=losers: \
                _reaction.message.id == message.id and _user not in _losers and not _user.bot

            while True:
                try:
                    reaction, user = await self.bot.wait_for('reaction_add', timeout=20,
                                                             check=check)
                    emoji_text = reaction.emoji.name if isinstance(reaction.emoji, discord.Emoji) \
                        else reaction.emoji

                    if emoji_text == answer:
                        await ctx.send(f'{user.mention} win :sanic: '
                                       f'\n'
                                       f'\n**Response was** {answer_name}:'
                                       f'\n**---**')
                        break
                    else:
                        losers.append(user)

                except asyncio.TimeoutError:
                    await ctx.send(f'**Time expired** '
                                   f'\n'
                                   f'\n**Response was** {answer_name}:'
                                   f'\n**---**')
                    break

            await asyncio.sleep(10)

    @quiadit.error
    async def poll_error(self, ctx, error):
        """Called on error in quiadit game"""
        logger.error('Error in poll ctx: %s, err: %s', ctx, error)
