"""User-facing error types and the global slash-command error handler.

Every command handler runs behind ``bot.tree.on_error = on_app_command_error``.
The handler guarantees the interaction always gets a reply and that no raw
traceback text ever reaches Discord — user-actionable problems (bad input, the DB
being down, a site artifact being unreachable) render as a small red embed, while
anything unexpected is logged and optionally alerted to a webhook.
"""

import logging

import aiohttp
import discord
from discord import app_commands

log = logging.getLogger("mythistone.bot")


class BotError(RuntimeError):
    """Base for errors whose message is safe to show the user."""

    user_message = "Something went wrong. Please try again in a moment."

    def __init__(self, user_message=None):
        super().__init__(user_message or self.user_message)
        if user_message:
            self.user_message = user_message


class ValidationError(BotError):
    """The user selected/typed something outside the allowed set."""

    user_message = "That selection isn't valid."


class DatabaseUnavailable(BotError):
    """A DB query failed or timed out."""

    user_message = (
        "The stats database is temporarily unavailable. Please try again shortly."
    )


class SiteDataError(BotError):
    """A published site artifact could not be fetched."""

    user_message = (
        "Couldn't reach mythistone.com data right now. Please try again shortly."
    )


class SeasonNotStarted(app_commands.CheckFailure):
    """The current season has no runs yet (pre-season gap / just after a wipe).

    Raised by the global season guard so every command short-circuits to a
    friendly "season hasn't started" embed instead of erroring on empty data. It
    subclasses CheckFailure so discord.py routes it to on_app_command_error.
    """


def error_embed(exc: Exception) -> discord.Embed:
    message = getattr(exc, "user_message", None) or BotError.user_message
    return discord.Embed(
        title="Something went wrong",
        description=message,
        colour=discord.Colour(0xFF4141),
    )


async def post_alert(session: aiohttp.ClientSession, webhook_url: str, text: str):
    """Fire-and-forget Discord webhook alert. Never raises."""
    if not webhook_url:
        return
    try:
        timeout = aiohttp.ClientTimeout(total=5)
        await session.post(webhook_url, json={"content": text[:1900]}, timeout=timeout)
    except Exception:
        log.debug("alert webhook post failed", exc_info=True)


async def _respond(interaction: discord.Interaction, embed: discord.Embed, ephemeral: bool):
    """Reply whether or not the interaction was already deferred/responded."""
    try:
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=ephemeral)
    except discord.HTTPException:
        log.warning("failed to deliver error embed to the user", exc_info=True)


async def on_app_command_error(
    interaction: discord.Interaction, error: app_commands.AppCommandError
):
    # Unwrap the wrapper discord.py puts around exceptions raised inside a command.
    if isinstance(error, app_commands.CommandInvokeError):
        error = error.original

    # Pre-season gap / post-wipe: show the season schedule instead of an error.
    if isinstance(error, SeasonNotStarted):
        from . import embeds  # local import: embeds imports config/emojis, not errors

        await _respond(interaction, embeds.season_not_started_embed(), ephemeral=False)
        return

    if isinstance(error, app_commands.CommandOnCooldown):
        embed = discord.Embed(
            title="Slow down a moment",
            description=f"Try again in {error.retry_after:.0f}s.",
            colour=discord.Colour(0xFF4141),
        )
        await _respond(interaction, embed, ephemeral=True)
        return

    if isinstance(error, ValidationError):
        await _respond(interaction, error_embed(error), ephemeral=True)
        return

    if isinstance(error, (DatabaseUnavailable, SiteDataError, BotError)):
        await _respond(interaction, error_embed(error), ephemeral=False)
        return

    # Unexpected: log the full traceback and alert, but show the user a generic
    # message with no internal detail.
    log.exception("unhandled command error", exc_info=error)
    client = interaction.client
    webhook_url = getattr(client, "webhook_url", None)
    session = getattr(getattr(client, "site_data", None), "session", None)
    if webhook_url and session is not None:
        cmd = interaction.command.qualified_name if interaction.command else "?"
        client.loop.create_task(
            post_alert(session, webhook_url, f"**bot command error** `/{cmd}`: {error!r}")
        )
    await _respond(interaction, error_embed(BotError()), ephemeral=False)
