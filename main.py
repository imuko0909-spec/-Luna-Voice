from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
import tempfile
import threading
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from flask import Flask
from gtts import gTTS

TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
DB_PATH = os.getenv("DATABASE_PATH", "data/yomiage.db")
PORT = int(os.getenv("PORT", "10000"))
MAX_READ_LENGTH = int(os.getenv("MAX_READ_LENGTH", "120"))
AUTO_DISCONNECT_SECONDS = int(os.getenv("AUTO_DISCONNECT_SECONDS", "30"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("yomiage")

Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
CUSTOM_EMOJI_RE = re.compile(r"<a?:\w+:\d+>")
MENTION_RE = re.compile(r"<@!?(\d+)>")
ROLE_RE = re.compile(r"<@&(\d+)>")
CHANNEL_RE = re.compile(r"<#(\d+)>")
SYMBOLS_ONLY_RE = re.compile(r"^[\W_]+$", re.UNICODE)


class Database:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()
        with self.connect() as con:
            con.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS settings(
                    guild_id INTEGER PRIMARY KEY,
                    volume REAL NOT NULL DEFAULT 1.0,
                    read_names INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS dictionary(
                    guild_id INTEGER NOT NULL,
                    word TEXT NOT NULL,
                    reading TEXT NOT NULL,
                    PRIMARY KEY(guild_id, word)
                );
                """
            )

    def connect(self):
        return sqlite3.connect(self.path, timeout=30)

    def settings(self, guild_id: int) -> tuple[float, bool]:
        with self.lock, self.connect() as con:
            con.execute("INSERT OR IGNORE INTO settings(guild_id) VALUES(?)", (guild_id,))
            row = con.execute("SELECT volume, read_names FROM settings WHERE guild_id=?", (guild_id,)).fetchone()
        return float(row[0]), bool(row[1])

    def set_volume(self, guild_id: int, volume: float):
        self.settings(guild_id)
        with self.lock, self.connect() as con:
            con.execute("UPDATE settings SET volume=? WHERE guild_id=?", (volume, guild_id))

    def set_read_names(self, guild_id: int, enabled: bool):
        self.settings(guild_id)
        with self.lock, self.connect() as con:
            con.execute("UPDATE settings SET read_names=? WHERE guild_id=?", (int(enabled), guild_id))

    def add_word(self, guild_id: int, word: str, reading: str):
        with self.lock, self.connect() as con:
            con.execute(
                "INSERT INTO dictionary VALUES(?,?,?) ON CONFLICT(guild_id,word) DO UPDATE SET reading=excluded.reading",
                (guild_id, word, reading),
            )

    def remove_word(self, guild_id: int, word: str) -> bool:
        with self.lock, self.connect() as con:
            cur = con.execute("DELETE FROM dictionary WHERE guild_id=? AND word=?", (guild_id, word))
            return cur.rowcount > 0

    def words(self, guild_id: int) -> list[tuple[str, str]]:
        with self.lock, self.connect() as con:
            rows = con.execute(
                "SELECT word, reading FROM dictionary WHERE guild_id=? ORDER BY LENGTH(word) DESC, word",
                (guild_id,),
            ).fetchall()
        return rows


db = Database(DB_PATH)


class Session:
    def __init__(self):
        self.text_channel_id: Optional[int] = None
        self.queue: asyncio.Queue[tuple[str, float]] = asyncio.Queue(maxsize=100)
        self.worker: Optional[asyncio.Task] = None
        self.auto_disconnect: Optional[asyncio.Task] = None


sessions: dict[int, Session] = defaultdict(Session)

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.voice_states = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)


def start_health_server():
    app = Flask(__name__)

    @app.get("/")
    def index():
        return {"status": "ok", "service": "yomiage-bot"}

    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=PORT),
        daemon=True,
    ).start()


def user_vc(member: discord.Member) -> Optional[discord.VoiceChannel]:
    if member.voice and isinstance(member.voice.channel, discord.VoiceChannel):
        return member.voice.channel
    return None


def human_count(channel: discord.VoiceChannel) -> int:
    return sum(not member.bot for member in channel.members)


def ensure_worker(guild_id: int):
    s = sessions[guild_id]
    if s.worker is None or s.worker.done():
        s.worker = asyncio.create_task(audio_worker(guild_id))


async def make_mp3(text: str) -> str:
    filename = str(Path(tempfile.gettempdir()) / f"yomiage_{uuid.uuid4().hex}.mp3")
    await asyncio.to_thread(lambda: gTTS(text=text, lang="ja", slow=False).save(filename))
    return filename


async def audio_worker(guild_id: int):
    s = sessions[guild_id]
    while True:
        text, volume = await s.queue.get()
        filename = None
        try:
            guild = bot.get_guild(guild_id)
            voice = guild.voice_client if guild else None
            if not voice or not voice.is_connected():
                continue
            filename = await make_mp3(text)
            source = discord.PCMVolumeTransformer(
                discord.FFmpegPCMAudio(filename, before_options="-nostdin", options="-vn -loglevel warning"),
                volume=volume,
            )
            done = asyncio.Event()
            loop = asyncio.get_running_loop()

            def after(error):
                if error:
                    log.error("playback error guild=%s: %s", guild_id, error)
                loop.call_soon_threadsafe(done.set)

            voice.play(source, after=after)
            await done.wait()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("TTS error guild=%s", guild_id)
        finally:
            if filename:
                Path(filename).unlink(missing_ok=True)
            s.queue.task_done()


async def connect_bot(guild: discord.Guild, member: discord.Member, text_channel) -> str:
    channel = user_vc(member)
    if channel is None:
        return "先に読み上げたいVCへ入ってから「接続」と送ってください。"

    me = guild.me
    if me is None:
        return "Bot情報を取得できませんでした。"
    perms = channel.permissions_for(me)
    if not perms.connect or not perms.speak:
        return "そのVCで必要な「接続・発言」権限がありません。"

    try:
        voice = guild.voice_client
        if voice and voice.is_connected():
            if voice.channel.id != channel.id:
                await voice.move_to(channel)
        else:
            await channel.connect(self_deaf=True, timeout=20, reconnect=True)

        s = sessions[guild.id]
        s.text_channel_id = text_channel.id
        if s.auto_disconnect and not s.auto_disconnect.done():
            s.auto_disconnect.cancel()
        ensure_worker(guild.id)
        return f"🔊 **{channel.name}** に接続しました。このチャンネルを読み上げます。"
    except Exception:
        log.exception("connect failed guild=%s", guild.id)
        return "VCへの接続に失敗しました。Botの権限とFFmpeg設定を確認してください。"


async def disconnect_bot(guild: discord.Guild):
    s = sessions[guild.id]
    if s.auto_disconnect and not s.auto_disconnect.done():
        s.auto_disconnect.cancel()
    voice = guild.voice_client
    if voice:
        voice.stop()
        await voice.disconnect(force=True)
    s.text_channel_id = None
    while not s.queue.empty():
        try:
            s.queue.get_nowait()
            s.queue.task_done()
        except asyncio.QueueEmpty:
            break


def clean_text(message: discord.Message) -> str:
    text = message.content.strip()
    if not text and message.attachments:
        text = "画像" if any((a.content_type or "").startswith("image/") for a in message.attachments) else "ファイル"
    if not text or (message.stickers and not message.content.strip()):
        return ""

    guild = message.guild
    text = URL_RE.sub(" URL省略 ", text)
    text = CUSTOM_EMOJI_RE.sub(" ", text)
    if guild:
        text = MENTION_RE.sub(lambda m: (guild.get_member(int(m.group(1))).display_name if guild.get_member(int(m.group(1))) else "メンション"), text)
        text = ROLE_RE.sub(lambda m: (guild.get_role(int(m.group(1))).name if guild.get_role(int(m.group(1))) else "ロール"), text)
        text = CHANNEL_RE.sub(lambda m: ((guild.get_channel(int(m.group(1))).name + "チャンネル") if guild.get_channel(int(m.group(1))) else "チャンネル"), text)
    for token in ("```", "**", "__", "~~", "||", "`", "*", "_", ">", "#"):
        text = text.replace(token, " ")
    text = re.sub(r"\s+", " ", text).strip()
    if not text or SYMBOLS_ONLY_RE.fullmatch(text):
        return ""
    if guild:
        for word, reading in db.words(guild.id):
            text = text.replace(word, reading)
    if len(text) > MAX_READ_LENGTH:
        text = text[:MAX_READ_LENGTH] + "、以下省略"
    return text


async def enqueue(message: discord.Message):
    text = clean_text(message)
    if not text or not message.guild:
        return
    volume, read_names = db.settings(message.guild.id)
    if read_names and isinstance(message.author, discord.Member):
        text = f"{message.author.display_name}、{text}"
    try:
        sessions[message.guild.id].queue.put_nowait((text, volume))
    except asyncio.QueueFull:
        log.warning("queue full guild=%s", message.guild.id)


@bot.event
async def on_ready():
    log.info("ログイン完了: %s", bot.user)
    try:
        synced = await bot.tree.sync()
        log.info("%s個のコマンドを同期しました", len(synced))
    except Exception:
        log.exception("command sync failed")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return
    content = message.content.strip()
    if content == "接続":
        result = await connect_bot(message.guild, message.author, message.channel)
        await message.reply(result, mention_author=False, delete_after=12)
        return
    if content == "切断":
        voice = message.guild.voice_client
        if not voice or not voice.is_connected():
            await message.reply("現在VCへ接続していません。", mention_author=False, delete_after=10)
            return
        same_vc = user_vc(message.author) == voice.channel
        if not same_vc and not message.author.guild_permissions.manage_channels:
            await message.reply("Botと同じVCにいる人か、チャンネル管理者だけ切断できます。", mention_author=False, delete_after=12)
            return
        await disconnect_bot(message.guild)
        await message.reply("🔇 読み上げを終了しました。", mention_author=False, delete_after=10)
        return

    s = sessions[message.guild.id]
    voice = message.guild.voice_client
    if voice and voice.is_connected() and s.text_channel_id == message.channel.id:
        await enqueue(message)
    await bot.process_commands(message)


@bot.event
async def on_voice_state_update(member, before, after):
    guild = member.guild
    voice = guild.voice_client
    if not voice or not voice.is_connected() or not isinstance(voice.channel, discord.VoiceChannel):
        return
    s = sessions[guild.id]
    if human_count(voice.channel) > 0:
        if s.auto_disconnect and not s.auto_disconnect.done():
            s.auto_disconnect.cancel()
        return
    if s.auto_disconnect and not s.auto_disconnect.done():
        return

    async def later():
        try:
            await asyncio.sleep(AUTO_DISCONNECT_SECONDS)
            vc = guild.voice_client
            if vc and isinstance(vc.channel, discord.VoiceChannel) and human_count(vc.channel) == 0:
                text_channel = guild.get_channel(s.text_channel_id) if s.text_channel_id else None
                await disconnect_bot(guild)
                if isinstance(text_channel, discord.TextChannel):
                    await text_channel.send("🔇 VCが無人になったため、自動で切断しました。")
        except asyncio.CancelledError:
            pass

    s.auto_disconnect = asyncio.create_task(later())


@bot.tree.command(name="接続", description="あなたがいるVCへ接続し、このチャンネルを読み上げます")
@app_commands.guild_only()
async def slash_connect(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    result = await connect_bot(interaction.guild, interaction.user, interaction.channel)
    await interaction.followup.send(result, ephemeral=True)


@bot.tree.command(name="切断", description="読み上げを終了してVCから切断します")
@app_commands.guild_only()
async def slash_disconnect(interaction: discord.Interaction):
    voice = interaction.guild.voice_client
    if not voice or not voice.is_connected():
        await interaction.response.send_message("現在VCへ接続していません。", ephemeral=True)
        return
    same_vc = user_vc(interaction.user) == voice.channel
    if not same_vc and not interaction.user.guild_permissions.manage_channels:
        await interaction.response.send_message("Botと同じVCにいる人か、チャンネル管理者だけ切断できます。", ephemeral=True)
        return
    await disconnect_bot(interaction.guild)
    await interaction.response.send_message("🔇 読み上げを終了しました。", ephemeral=True)


@bot.tree.command(name="辞書追加", description="読み方を登録します")
@app_commands.describe(word="置き換え前", reading="読み方")
@app_commands.guild_only()
async def add_dictionary(interaction: discord.Interaction, word: str, reading: str):
    db.add_word(interaction.guild.id, word.strip(), reading.strip())
    await interaction.response.send_message(f"📖 `{word}` → `{reading}` を登録しました。", ephemeral=True)


@bot.tree.command(name="辞書削除", description="登録した読み方を削除します")
@app_commands.guild_only()
async def remove_dictionary(interaction: discord.Interaction, word: str):
    removed = db.remove_word(interaction.guild.id, word.strip())
    text = f"🗑️ `{word}` を削除しました。" if removed else f"`{word}` は登録されていません。"
    await interaction.response.send_message(text, ephemeral=True)


@bot.tree.command(name="辞書一覧", description="読み上げ辞書を表示します")
@app_commands.guild_only()
async def list_dictionary(interaction: discord.Interaction):
    rows = db.words(interaction.guild.id)
    if not rows:
        await interaction.response.send_message("辞書はまだ空です。", ephemeral=True)
        return
    lines = [f"`{w}` → `{r}`" for w, r in rows[:40]]
    await interaction.response.send_message("📚 **読み上げ辞書**\n" + "\n".join(lines), ephemeral=True)


@bot.tree.command(name="音量", description="読み上げ音量を10〜200％で設定します")
@app_commands.guild_only()
async def volume(interaction: discord.Interaction, percent: app_commands.Range[int, 10, 200]):
    db.set_volume(interaction.guild.id, percent / 100)
    await interaction.response.send_message(f"🔊 音量を **{percent}%** にしました。", ephemeral=True)


@bot.tree.command(name="名前読み", description="投稿者名を読むか設定します")
@app_commands.guild_only()
async def read_names(interaction: discord.Interaction, enabled: bool):
    db.set_read_names(interaction.guild.id, enabled)
    await interaction.response.send_message(f"👤 名前読みを **{'オン' if enabled else 'オフ'}** にしました。", ephemeral=True)


@bot.tree.command(name="読み上げ設定", description="現在の設定を表示します")
@app_commands.guild_only()
async def show_settings(interaction: discord.Interaction):
    volume_value, read_names_value = db.settings(interaction.guild.id)
    s = sessions[interaction.guild.id]
    voice = interaction.guild.voice_client
    vc = voice.channel.name if voice and voice.is_connected() else "未接続"
    tc = interaction.guild.get_channel(s.text_channel_id) if s.text_channel_id else None
    tc_text = tc.mention if isinstance(tc, discord.TextChannel) else "未設定"
    await interaction.response.send_message(
        f"⚙️ **読み上げ設定**\nVC：`{vc}`\n対象テキスト：{tc_text}\n音量：`{round(volume_value*100)}%`\n名前読み：`{'オン' if read_names_value else 'オフ'}`",
        ephemeral=True,
    )


def main():
    if not TOKEN:
        raise RuntimeError("環境変数 DISCORD_TOKEN を設定してください。")
    start_health_server()
    bot.run(TOKEN, log_handler=None)


if __name__ == "__main__":
    main()
