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
import edge_tts
from discord import app_commands
from discord.ext import commands
from flask import Flask


# =========================================================
# 基本設定
# =========================================================

TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
DB_PATH = os.getenv("DATABASE_PATH", "data/yomiage.db")
PORT = int(os.getenv("PORT", "10000"))
MAX_READ_LENGTH = int(os.getenv("MAX_READ_LENGTH", "120"))
AUTO_DISCONNECT_SECONDS = int(os.getenv("AUTO_DISCONNECT_SECONDS", "30"))

DEFAULT_VOICE = os.getenv(
    "DEFAULT_VOICE",
    "ja-JP-NanamiNeural",
).strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("hachifure-yomiage")

Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
CUSTOM_EMOJI_RE = re.compile(r"<a?:\w+:\d+>")
MENTION_RE = re.compile(r"<@!?(\d+)>")
ROLE_RE = re.compile(r"<@&(\d+)>")
CHANNEL_RE = re.compile(r"<#(\d+)>")
SYMBOLS_ONLY_RE = re.compile(r"^[\W_]+$", re.UNICODE)
WHITESPACE_RE = re.compile(r"\s+")


# =========================================================
# 声プリセット
# =========================================================
# edge-ttsの日本語音声をベースに、速度・高さで雰囲気を変更します。
# 「ロリ風」は年齢を再現するものではなく、高めで可愛い声のプリセットです。

VOICE_PROFILES = {
    "お姉さん": {
        "voice": "ja-JP-NanamiNeural",
        "rate": "-5%",
        "pitch": "-8Hz",
        "description": "落ち着いた女性声",
    },
    "女性": {
        "voice": "ja-JP-NanamiNeural",
        "rate": "+0%",
        "pitch": "+0Hz",
        "description": "標準的な女性声",
    },
    "かわいい": {
        "voice": "ja-JP-NanamiNeural",
        "rate": "+5%",
        "pitch": "+25Hz",
        "description": "明るく高めの女性声",
    },
    "ロリ風": {
        "voice": "ja-JP-NanamiNeural",
        "rate": "+8%",
        "pitch": "+45Hz",
        "description": "かなり高めの可愛い声",
    },
    "男性": {
        "voice": "ja-JP-KeitaNeural",
        "rate": "+0%",
        "pitch": "+0Hz",
        "description": "標準的な男性声",
    },
    "イケボ": {
        "voice": "ja-JP-KeitaNeural",
        "rate": "-8%",
        "pitch": "-22Hz",
        "description": "低めで落ち着いた男性声",
    },
    "少年風": {
        "voice": "ja-JP-KeitaNeural",
        "rate": "+8%",
        "pitch": "+28Hz",
        "description": "高めで軽快な男性声",
    },
}

PROFILE_NAMES = list(VOICE_PROFILES.keys())


# =========================================================
# データベース
# =========================================================

class Database:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=30)

    def _initialize(self) -> None:
        with self.lock, self.connect() as con:
            con.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS settings(
                    guild_id INTEGER PRIMARY KEY,
                    volume REAL NOT NULL DEFAULT 1.0,
                    read_names INTEGER NOT NULL DEFAULT 1,
                    edge_profile TEXT NOT NULL DEFAULT '女性',
                    edge_voice TEXT NOT NULL DEFAULT 'ja-JP-NanamiNeural',
                    edge_rate INTEGER NOT NULL DEFAULT 0,
                    edge_pitch INTEGER NOT NULL DEFAULT 0,
                    read_join_leave INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS user_voice_edge(
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    profile TEXT NOT NULL,
                    PRIMARY KEY(guild_id, user_id)
                );

                CREATE TABLE IF NOT EXISTS dictionary(
                    guild_id INTEGER NOT NULL,
                    word TEXT NOT NULL,
                    reading TEXT NOT NULL,
                    PRIMARY KEY(guild_id, word)
                );
                """
            )

            # 以前のDBをそのまま引き継げるように、不足列だけ追加します。
            migrations = (
                "ALTER TABLE settings ADD COLUMN edge_profile TEXT NOT NULL DEFAULT '女性'",
                "ALTER TABLE settings ADD COLUMN edge_voice TEXT NOT NULL DEFAULT 'ja-JP-NanamiNeural'",
                "ALTER TABLE settings ADD COLUMN edge_rate INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE settings ADD COLUMN edge_pitch INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE settings ADD COLUMN read_join_leave INTEGER NOT NULL DEFAULT 1",
            )
            for sql in migrations:
                try:
                    con.execute(sql)
                except sqlite3.OperationalError:
                    pass
            con.commit()

    def settings(self, guild_id: int) -> dict:
        with self.lock, self.connect() as con:
            con.execute(
                """
                INSERT OR IGNORE INTO settings(
                    guild_id, edge_voice
                ) VALUES(?, ?)
                """,
                (guild_id, DEFAULT_VOICE),
            )
            row = con.execute(
                """
                SELECT
                    volume,
                    read_names,
                    edge_profile,
                    edge_voice,
                    edge_rate,
                    edge_pitch,
                    read_join_leave
                FROM settings
                WHERE guild_id=?
                """,
                (guild_id,),
            ).fetchone()
            con.commit()

        if row is None:
            raise RuntimeError("設定を取得できませんでした。")

        return {
            "volume": float(row[0]),
            "read_names": bool(row[1]),
            "profile": str(row[2]),
            "voice": str(row[3]),
            "rate": int(row[4]),
            "pitch": int(row[5]),
            "read_join_leave": bool(row[6]),
        }

    def update_setting(self, guild_id: int, column: str, value) -> None:
        allowed = {
            "volume",
            "read_names",
            "edge_profile",
            "edge_voice",
            "edge_rate",
            "edge_pitch",
            "read_join_leave",
        }
        if column not in allowed:
            raise ValueError(f"変更できない設定です: {column}")

        self.settings(guild_id)
        with self.lock, self.connect() as con:
            con.execute(
                f"UPDATE settings SET {column}=? WHERE guild_id=?",
                (value, guild_id),
            )
            con.commit()

    def set_profile(self, guild_id: int, profile: str) -> None:
        preset = VOICE_PROFILES[profile]
        with self.lock, self.connect() as con:
            con.execute(
                """
                INSERT OR IGNORE INTO settings(guild_id)
                VALUES(?)
                """,
                (guild_id,),
            )
            con.execute(
                """
                UPDATE settings
                SET edge_profile=?, edge_voice=?, edge_rate=0, edge_pitch=0
                WHERE guild_id=?
                """,
                (profile, preset["voice"], guild_id),
            )
            con.commit()

    def set_user_profile(
        self,
        guild_id: int,
        user_id: int,
        profile: str,
    ) -> None:
        with self.lock, self.connect() as con:
            con.execute(
                """
                INSERT INTO user_voice_edge(guild_id, user_id, profile)
                VALUES(?, ?, ?)
                ON CONFLICT(guild_id, user_id)
                DO UPDATE SET profile=excluded.profile
                """,
                (guild_id, user_id, profile),
            )
            con.commit()

    def get_user_profile(
        self,
        guild_id: int,
        user_id: int,
    ) -> Optional[str]:
        with self.lock, self.connect() as con:
            row = con.execute(
                """
                SELECT profile
                FROM user_voice_edge
                WHERE guild_id=? AND user_id=?
                """,
                (guild_id, user_id),
            ).fetchone()
        return str(row[0]) if row else None

    def remove_user_profile(
        self,
        guild_id: int,
        user_id: int,
    ) -> bool:
        with self.lock, self.connect() as con:
            cur = con.execute(
                """
                DELETE FROM user_voice_edge
                WHERE guild_id=? AND user_id=?
                """,
                (guild_id, user_id),
            )
            con.commit()
            return cur.rowcount > 0

    def add_word(self, guild_id: int, word: str, reading: str) -> None:
        with self.lock, self.connect() as con:
            con.execute(
                """
                INSERT INTO dictionary(guild_id, word, reading)
                VALUES(?, ?, ?)
                ON CONFLICT(guild_id, word)
                DO UPDATE SET reading=excluded.reading
                """,
                (guild_id, word, reading),
            )
            con.commit()

    def remove_word(self, guild_id: int, word: str) -> bool:
        with self.lock, self.connect() as con:
            cur = con.execute(
                """
                DELETE FROM dictionary
                WHERE guild_id=? AND word=?
                """,
                (guild_id, word),
            )
            con.commit()
            return cur.rowcount > 0

    def words(self, guild_id: int) -> list[tuple[str, str]]:
        with self.lock, self.connect() as con:
            rows = con.execute(
                """
                SELECT word, reading
                FROM dictionary
                WHERE guild_id=?
                ORDER BY LENGTH(word) DESC, word
                """,
                (guild_id,),
            ).fetchall()
        return [(str(word), str(reading)) for word, reading in rows]


db = Database(DB_PATH)


# =========================================================
# 読み上げセッション
# =========================================================

class Session:
    def __init__(self):
        self.text_channel_id: Optional[int] = None
        self.queue: asyncio.Queue[
            tuple[str, float, str, str, str]
        ] = asyncio.Queue(maxsize=100)
        self.worker: Optional[asyncio.Task] = None
        self.auto_disconnect: Optional[asyncio.Task] = None


sessions: dict[int, Session] = defaultdict(Session)


# =========================================================
# Bot・Render
# =========================================================

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.voice_states = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)


def start_health_server() -> None:
    app = Flask(__name__)

    @app.get("/")
    def index():
        return {
            "status": "ok",
            "service": "hachifure-yomiage-bot",
            "tts": "edge-tts",
        }

    threading.Thread(
        target=lambda: app.run(
            host="0.0.0.0",
            port=PORT,
            use_reloader=False,
        ),
        daemon=True,
    ).start()


# =========================================================
# 音声生成
# =========================================================

def format_percent(value: int) -> str:
    return f"{value:+d}%"


def format_hz(value: int) -> str:
    return f"{value:+d}Hz"


def resolve_voice_settings(
    guild_id: int,
    user_id: Optional[int],
) -> tuple[str, str, str, str]:
    cfg = db.settings(guild_id)
    profile_name = cfg["profile"]

    if user_id is not None:
        personal = db.get_user_profile(guild_id, user_id)
        if personal in VOICE_PROFILES:
            profile_name = personal

    preset = VOICE_PROFILES.get(
        profile_name,
        VOICE_PROFILES["女性"],
    )

    voice = preset["voice"]
    base_rate = int(preset["rate"].replace("%", ""))
    base_pitch = int(preset["pitch"].replace("Hz", ""))

    final_rate = max(-50, min(100, base_rate + cfg["rate"]))
    final_pitch = max(-100, min(100, base_pitch + cfg["pitch"]))

    return (
        profile_name,
        voice,
        format_percent(final_rate),
        format_hz(final_pitch),
    )


async def make_tts(
    text: str,
    voice: str,
    rate: str,
    pitch: str,
) -> str:
    filename = str(
        Path(tempfile.gettempdir())
        / f"edge_tts_{uuid.uuid4().hex}.mp3"
    )

    communicate = edge_tts.Communicate(
        text=text,
        voice=voice,
        rate=rate,
        pitch=pitch,
    )
    await communicate.save(filename)
    return filename


def ensure_worker(guild_id: int) -> None:
    session = sessions[guild_id]
    if session.worker is None or session.worker.done():
        session.worker = asyncio.create_task(audio_worker(guild_id))


async def clear_queue(guild_id: int) -> None:
    session = sessions[guild_id]
    while not session.queue.empty():
        try:
            session.queue.get_nowait()
            session.queue.task_done()
        except asyncio.QueueEmpty:
            break


async def audio_worker(guild_id: int) -> None:
    session = sessions[guild_id]

    while True:
        text, volume, voice_name, rate, pitch = await session.queue.get()
        filename: Optional[str] = None

        try:
            guild = bot.get_guild(guild_id)
            voice_client = guild.voice_client if guild else None

            if not voice_client or not voice_client.is_connected():
                continue

            filename = await make_tts(
                text,
                voice_name,
                rate,
                pitch,
            )

            source = discord.PCMVolumeTransformer(
                discord.FFmpegPCMAudio(
                    filename,
                    before_options="-nostdin",
                    options="-vn -loglevel warning",
                ),
                volume=volume,
            )

            done = asyncio.Event()
            loop = asyncio.get_running_loop()

            def after(error: Optional[Exception]) -> None:
                if error:
                    log.error(
                        "再生エラー guild=%s: %s",
                        guild_id,
                        error,
                    )
                loop.call_soon_threadsafe(done.set)

            while voice_client.is_playing():
                await asyncio.sleep(0.05)

            voice_client.play(source, after=after)
            await done.wait()

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("TTSエラー guild=%s", guild_id)
        finally:
            if filename:
                try:
                    Path(filename).unlink(missing_ok=True)
                except Exception:
                    log.exception("一時ファイルを削除できませんでした。")
            session.queue.task_done()


# =========================================================
# 接続・切断
# =========================================================

def user_vc(
    member: discord.Member,
) -> Optional[discord.VoiceChannel]:
    if (
        member.voice
        and isinstance(member.voice.channel, discord.VoiceChannel)
    ):
        return member.voice.channel
    return None


def human_count(channel: discord.VoiceChannel) -> int:
    return sum(1 for member in channel.members if not member.bot)


async def connect_bot(
    guild: discord.Guild,
    member: discord.Member,
    text_channel: discord.abc.Messageable,
) -> str:
    channel = user_vc(member)
    if channel is None:
        return "先に読み上げたいVCへ入ってから接続してください。"

    me = guild.me
    if me is None:
        return "Bot情報を取得できませんでした。"

    permissions = channel.permissions_for(me)
    if not permissions.connect or not permissions.speak:
        return "そのVCで必要な「接続・発言」権限がありません。"

    try:
        voice_client = guild.voice_client

        if voice_client and voice_client.is_connected():
            if voice_client.channel.id != channel.id:
                await voice_client.move_to(channel)
        else:
            await channel.connect(
                self_deaf=True,
                timeout=20,
                reconnect=True,
            )

        session = sessions[guild.id]
        session.text_channel_id = text_channel.id

        if (
            session.auto_disconnect
            and not session.auto_disconnect.done()
        ):
            session.auto_disconnect.cancel()

        ensure_worker(guild.id)
        cfg = db.settings(guild.id)

        return (
            f"🔊 **{channel.name}** に接続しました。\n"
            f"このチャンネルを読み上げます。\n"
            f"🎤 標準の声：**{cfg['profile']}**"
        )

    except Exception:
        log.exception("VC接続失敗 guild=%s", guild.id)
        return (
            "VCへの接続に失敗しました。"
            "Botの接続・発言権限とRenderのFFmpegを確認してください。"
        )


async def disconnect_bot(guild: discord.Guild) -> None:
    session = sessions[guild.id]

    if (
        session.auto_disconnect
        and not session.auto_disconnect.done()
    ):
        session.auto_disconnect.cancel()

    voice_client = guild.voice_client
    if voice_client:
        voice_client.stop()
        await voice_client.disconnect(force=True)

    session.text_channel_id = None
    await clear_queue(guild.id)


# =========================================================
# 文章整形
# =========================================================

def replace_mentions(text: str, guild: discord.Guild) -> str:
    def member_name(match: re.Match) -> str:
        member = guild.get_member(int(match.group(1)))
        return member.display_name if member else "メンション"

    def role_name(match: re.Match) -> str:
        role = guild.get_role(int(match.group(1)))
        return role.name if role else "ロール"

    def channel_name(match: re.Match) -> str:
        channel = guild.get_channel(int(match.group(1)))
        return f"{channel.name}チャンネル" if channel else "チャンネル"

    text = MENTION_RE.sub(member_name, text)
    text = ROLE_RE.sub(role_name, text)
    text = CHANNEL_RE.sub(channel_name, text)
    return text


def clean_text(message: discord.Message) -> str:
    text = message.content.strip()

    if not text and message.attachments:
        is_image = any(
            (attachment.content_type or "").startswith("image/")
            for attachment in message.attachments
        )
        text = "画像" if is_image else "ファイル"

    if not text and message.stickers:
        text = f"{message.stickers[0].name}のスタンプ"

    if not text:
        return ""

    guild = message.guild
    text = URL_RE.sub(" URL省略 ", text)
    text = CUSTOM_EMOJI_RE.sub(" 絵文字 ", text)

    if guild:
        text = replace_mentions(text, guild)

    for token in (
        "```", "**", "__", "~~", "||",
        "`", "*", "_", ">", "#",
    ):
        text = text.replace(token, " ")

    text = WHITESPACE_RE.sub(" ", text).strip()

    if not text or SYMBOLS_ONLY_RE.fullmatch(text):
        return ""

    if guild:
        for word, reading in db.words(guild.id):
            text = text.replace(word, reading)

    if len(text) > MAX_READ_LENGTH:
        text = text[:MAX_READ_LENGTH] + "、以下省略"

    return text


async def enqueue_text(
    guild: discord.Guild,
    text: str,
    *,
    author_id: Optional[int] = None,
    author_name: Optional[str] = None,
) -> None:
    if not text:
        return

    cfg = db.settings(guild.id)
    _, voice_name, rate, pitch = resolve_voice_settings(
        guild.id,
        author_id,
    )

    if author_name and cfg["read_names"]:
        text = f"{author_name}、{text}"

    ensure_worker(guild.id)

    try:
        sessions[guild.id].queue.put_nowait(
            (
                text,
                cfg["volume"],
                voice_name,
                rate,
                pitch,
            )
        )
    except asyncio.QueueFull:
        log.warning("読み上げ待機列が満杯です guild=%s", guild.id)


async def enqueue(message: discord.Message) -> None:
    if not message.guild:
        return

    text = clean_text(message)
    if not text:
        return

    author_name = (
        message.author.display_name
        if isinstance(message.author, discord.Member)
        else None
    )

    await enqueue_text(
        message.guild,
        text,
        author_id=message.author.id,
        author_name=author_name,
    )


# =========================================================
# イベント
# =========================================================

@bot.event
async def on_ready() -> None:
    log.info("ログイン完了: %s", bot.user)
    try:
        synced = await bot.tree.sync()
        log.info("%s個のコマンドを同期しました", len(synced))
    except Exception:
        log.exception("コマンド同期に失敗しました。")


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.bot or not message.guild:
        return

    content = message.content.strip()

    if content == "接続":
        if not isinstance(message.author, discord.Member):
            return
        result = await connect_bot(
            message.guild,
            message.author,
            message.channel,
        )
        await message.reply(
            result,
            mention_author=False,
            delete_after=15,
        )
        return

    if content == "切断":
        voice_client = message.guild.voice_client

        if not voice_client or not voice_client.is_connected():
            await message.reply(
                "現在VCへ接続していません。",
                mention_author=False,
                delete_after=10,
            )
            return

        same_vc = (
            isinstance(message.author, discord.Member)
            and user_vc(message.author) == voice_client.channel
        )
        can_manage = (
            isinstance(message.author, discord.Member)
            and message.author.guild_permissions.manage_channels
        )

        if not same_vc and not can_manage:
            await message.reply(
                "Botと同じVCにいる人か、"
                "チャンネル管理者だけ切断できます。",
                mention_author=False,
                delete_after=12,
            )
            return

        await disconnect_bot(message.guild)
        await message.reply(
            "🔇 読み上げを終了しました。",
            mention_author=False,
            delete_after=10,
        )
        return

    session = sessions[message.guild.id]
    voice_client = message.guild.voice_client

    if (
        voice_client
        and voice_client.is_connected()
        and session.text_channel_id == message.channel.id
    ):
        await enqueue(message)

    await bot.process_commands(message)


@bot.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
) -> None:
    guild = member.guild
    voice_client = guild.voice_client

    if (
        member.bot
        or not voice_client
        or not voice_client.is_connected()
        or not isinstance(voice_client.channel, discord.VoiceChannel)
    ):
        return

    session = sessions[guild.id]
    cfg = db.settings(guild.id)

    if cfg["read_join_leave"]:
        joined = (
            after.channel == voice_client.channel
            and before.channel != voice_client.channel
        )
        left = (
            before.channel == voice_client.channel
            and after.channel != voice_client.channel
        )

        if joined:
            await enqueue_text(
                guild,
                f"{member.display_name}さんが入室しました",
                author_id=member.id,
            )
        elif left:
            await enqueue_text(
                guild,
                f"{member.display_name}さんが退室しました",
                author_id=member.id,
            )

    if human_count(voice_client.channel) > 0:
        if (
            session.auto_disconnect
            and not session.auto_disconnect.done()
        ):
            session.auto_disconnect.cancel()
        return

    if (
        session.auto_disconnect
        and not session.auto_disconnect.done()
    ):
        return

    async def later() -> None:
        try:
            await asyncio.sleep(AUTO_DISCONNECT_SECONDS)
            current = guild.voice_client

            if (
                current
                and isinstance(current.channel, discord.VoiceChannel)
                and human_count(current.channel) == 0
            ):
                text_channel = (
                    guild.get_channel(session.text_channel_id)
                    if session.text_channel_id
                    else None
                )

                await disconnect_bot(guild)

                if isinstance(text_channel, discord.TextChannel):
                    await text_channel.send(
                        "🔇 VCが無人になったため、自動で切断しました。"
                    )
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("自動切断に失敗しました guild=%s", guild.id)

    session.auto_disconnect = asyncio.create_task(later())


# =========================================================
# 選択肢
# =========================================================

async def profile_choices(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    del interaction
    key = current.strip().lower()
    output = []

    for name, data in VOICE_PROFILES.items():
        label = f"{name}｜{data['description']}"
        if not key or key in label.lower():
            output.append(
                app_commands.Choice(
                    name=label[:100],
                    value=name,
                )
            )
    return output[:25]


# =========================================================
# 接続コマンド
# =========================================================

@bot.tree.command(
    name="接続",
    description="あなたがいるVCへ接続します",
)
@app_commands.guild_only()
async def slash_connect(interaction: discord.Interaction) -> None:
    if (
        not interaction.guild
        or not isinstance(interaction.user, discord.Member)
    ):
        return

    await interaction.response.defer(ephemeral=True)
    result = await connect_bot(
        interaction.guild,
        interaction.user,
        interaction.channel,
    )
    await interaction.followup.send(result, ephemeral=True)


@bot.tree.command(
    name="切断",
    description="読み上げを終了してVCから切断します",
)
@app_commands.guild_only()
async def slash_disconnect(interaction: discord.Interaction) -> None:
    if (
        not interaction.guild
        or not isinstance(interaction.user, discord.Member)
    ):
        return

    voice_client = interaction.guild.voice_client

    if not voice_client or not voice_client.is_connected():
        await interaction.response.send_message(
            "現在VCへ接続していません。",
            ephemeral=True,
        )
        return

    same_vc = user_vc(interaction.user) == voice_client.channel

    if (
        not same_vc
        and not interaction.user.guild_permissions.manage_channels
    ):
        await interaction.response.send_message(
            "Botと同じVCにいる人か、"
            "チャンネル管理者だけ切断できます。",
            ephemeral=True,
        )
        return

    await disconnect_bot(interaction.guild)
    await interaction.response.send_message(
        "🔇 読み上げを終了しました。",
        ephemeral=True,
    )


# =========================================================
# 声コマンド
# =========================================================

@bot.tree.command(
    name="声",
    description="サーバー標準の声を変更します",
)
@app_commands.describe(profile="声の雰囲気を選択")
@app_commands.autocomplete(profile=profile_choices)
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def set_voice(
    interaction: discord.Interaction,
    profile: str,
) -> None:
    if not interaction.guild:
        return

    if profile not in VOICE_PROFILES:
        await interaction.response.send_message(
            "候補から声を選んでください。",
            ephemeral=True,
        )
        return

    db.set_profile(interaction.guild.id, profile)
    data = VOICE_PROFILES[profile]

    await interaction.response.send_message(
        f"🎤 標準の声を **{profile}** に変更しました。\n"
        f"{data['description']}",
        ephemeral=True,
    )


@bot.tree.command(
    name="個人声",
    description="自分の投稿を読む声を変更します",
)
@app_commands.describe(profile="自分用の声を選択")
@app_commands.autocomplete(profile=profile_choices)
@app_commands.guild_only()
async def personal_voice(
    interaction: discord.Interaction,
    profile: str,
) -> None:
    if not interaction.guild:
        return

    if profile not in VOICE_PROFILES:
        await interaction.response.send_message(
            "候補から声を選んでください。",
            ephemeral=True,
        )
        return

    db.set_user_profile(
        interaction.guild.id,
        interaction.user.id,
        profile,
    )

    await interaction.response.send_message(
        f"🗣️ あなたの声を **{profile}** にしました。",
        ephemeral=True,
    )


@bot.tree.command(
    name="個人声解除",
    description="自分専用の声を解除します",
)
@app_commands.guild_only()
async def personal_voice_remove(
    interaction: discord.Interaction,
) -> None:
    if not interaction.guild:
        return

    removed = db.remove_user_profile(
        interaction.guild.id,
        interaction.user.id,
    )

    await interaction.response.send_message(
        (
            "個人声を解除しました。"
            if removed
            else "個人声は設定されていません。"
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="声一覧",
    description="選べる声の一覧を表示します",
)
@app_commands.guild_only()
async def voice_list(interaction: discord.Interaction) -> None:
    lines = [
        f"**{name}**：{data['description']}"
        for name, data in VOICE_PROFILES.items()
    ]

    await interaction.response.send_message(
        "🎤 **声一覧**\n" + "\n".join(lines),
        ephemeral=True,
    )


@bot.tree.command(
    name="話速",
    description="標準プリセットからの速度補正を設定します",
)
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def speed_cmd(
    interaction: discord.Interaction,
    percent: app_commands.Range[int, -50, 100],
) -> None:
    if not interaction.guild:
        return

    db.update_setting(
        interaction.guild.id,
        "edge_rate",
        int(percent),
    )

    await interaction.response.send_message(
        f"⏩ 速度補正を **{percent:+d}%** にしました。",
        ephemeral=True,
    )


@bot.tree.command(
    name="音程",
    description="標準プリセットからの高さ補正を設定します",
)
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def pitch_cmd(
    interaction: discord.Interaction,
    hz: app_commands.Range[int, -100, 100],
) -> None:
    if not interaction.guild:
        return

    db.update_setting(
        interaction.guild.id,
        "edge_pitch",
        int(hz),
    )

    await interaction.response.send_message(
        f"🎵 高さ補正を **{hz:+d}Hz** にしました。",
        ephemeral=True,
    )


# =========================================================
# 音量・名前・入退室
# =========================================================

@bot.tree.command(
    name="音量",
    description="読み上げ音量を10〜200％で設定します",
)
@app_commands.guild_only()
async def volume(
    interaction: discord.Interaction,
    percent: app_commands.Range[int, 10, 200],
) -> None:
    if not interaction.guild:
        return

    db.update_setting(
        interaction.guild.id,
        "volume",
        percent / 100,
    )

    await interaction.response.send_message(
        f"🔊 音量を **{percent}%** にしました。",
        ephemeral=True,
    )


@bot.tree.command(
    name="名前読み",
    description="投稿者名を読むか設定します",
)
@app_commands.guild_only()
async def read_names(
    interaction: discord.Interaction,
    enabled: bool,
) -> None:
    if not interaction.guild:
        return

    db.update_setting(
        interaction.guild.id,
        "read_names",
        int(enabled),
    )

    await interaction.response.send_message(
        f"👤 名前読みを **{'オン' if enabled else 'オフ'}** にしました。",
        ephemeral=True,
    )


@bot.tree.command(
    name="入退室読み",
    description="VCの入退室を読み上げるか設定します",
)
@app_commands.guild_only()
async def read_join_leave(
    interaction: discord.Interaction,
    enabled: bool,
) -> None:
    if not interaction.guild:
        return

    db.update_setting(
        interaction.guild.id,
        "read_join_leave",
        int(enabled),
    )

    await interaction.response.send_message(
        f"🚪 入退室読みを **{'オン' if enabled else 'オフ'}** にしました。",
        ephemeral=True,
    )


# =========================================================
# 辞書
# =========================================================

@bot.tree.command(
    name="辞書追加",
    description="読み方を登録します",
)
@app_commands.describe(
    word="置き換え前の文字",
    reading="読み方",
)
@app_commands.guild_only()
async def add_dictionary(
    interaction: discord.Interaction,
    word: str,
    reading: str,
) -> None:
    if not interaction.guild:
        return

    word = word.strip()
    reading = reading.strip()

    if not word or not reading:
        await interaction.response.send_message(
            "単語と読み方を入力してください。",
            ephemeral=True,
        )
        return

    if len(word) > 50 or len(reading) > 100:
        await interaction.response.send_message(
            "単語は50文字以内、読み方は100文字以内にしてください。",
            ephemeral=True,
        )
        return

    db.add_word(interaction.guild.id, word, reading)

    await interaction.response.send_message(
        f"📖 `{word}` → `{reading}` を登録しました。",
        ephemeral=True,
    )


@bot.tree.command(
    name="辞書削除",
    description="登録した読み方を削除します",
)
@app_commands.guild_only()
async def remove_dictionary(
    interaction: discord.Interaction,
    word: str,
) -> None:
    if not interaction.guild:
        return

    word = word.strip()
    removed = db.remove_word(interaction.guild.id, word)

    await interaction.response.send_message(
        (
            f"🗑️ `{word}` を削除しました。"
            if removed
            else f"`{word}` は登録されていません。"
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="辞書一覧",
    description="読み上げ辞書を表示します",
)
@app_commands.guild_only()
async def list_dictionary(
    interaction: discord.Interaction,
) -> None:
    if not interaction.guild:
        return

    rows = db.words(interaction.guild.id)

    if not rows:
        await interaction.response.send_message(
            "辞書はまだ空です。",
            ephemeral=True,
        )
        return

    lines = [f"`{word}` → `{reading}`" for word, reading in rows[:40]]

    if len(rows) > 40:
        lines.append(f"ほか {len(rows) - 40} 件")

    await interaction.response.send_message(
        "📚 **読み上げ辞書**\n" + "\n".join(lines),
        ephemeral=True,
    )


# =========================================================
# 設定表示・テスト
# =========================================================

@bot.tree.command(
    name="読み上げ設定",
    description="現在の設定を表示します",
)
@app_commands.guild_only()
async def show_settings(
    interaction: discord.Interaction,
) -> None:
    if not interaction.guild:
        return

    cfg = db.settings(interaction.guild.id)
    session = sessions[interaction.guild.id]
    voice_client = interaction.guild.voice_client

    vc_name = (
        voice_client.channel.name
        if voice_client and voice_client.is_connected()
        else "未接続"
    )

    text_channel = (
        interaction.guild.get_channel(session.text_channel_id)
        if session.text_channel_id
        else None
    )
    text_channel_text = (
        text_channel.mention
        if isinstance(text_channel, discord.TextChannel)
        else "未設定"
    )

    personal = db.get_user_profile(
        interaction.guild.id,
        interaction.user.id,
    ) or "未設定"

    await interaction.response.send_message(
        "⚙️ **読み上げ設定**\n"
        f"VC：`{vc_name}`\n"
        f"対象テキスト：{text_channel_text}\n"
        f"標準の声：`{cfg['profile']}`\n"
        f"あなたの個人声：`{personal}`\n"
        f"音量：`{round(cfg['volume'] * 100)}%`\n"
        f"名前読み：`{'オン' if cfg['read_names'] else 'オフ'}`\n"
        f"入退室読み：`{'オン' if cfg['read_join_leave'] else 'オフ'}`\n"
        f"速度補正：`{cfg['rate']:+d}%`\n"
        f"高さ補正：`{cfg['pitch']:+d}Hz`\n"
        "TTS：`edge-tts`",
        ephemeral=True,
    )


@bot.tree.command(
    name="音声テスト",
    description="現在選択中の声で短い音声を読み上げます",
)
@app_commands.guild_only()
async def voice_test(
    interaction: discord.Interaction,
) -> None:
    if not interaction.guild:
        return

    voice_client = interaction.guild.voice_client
    if not voice_client or not voice_client.is_connected():
        await interaction.response.send_message(
            "先に `/接続` でVCへ接続してください。",
            ephemeral=True,
        )
        return

    await enqueue_text(
        interaction.guild,
        "音声テストです。聞こえていますか？",
        author_id=interaction.user.id,
    )

    await interaction.response.send_message(
        "🔊 音声テストを再生します。",
        ephemeral=True,
    )


# =========================================================
# エラー処理
# =========================================================

@set_voice.error
@speed_cmd.error
@pitch_cmd.error
async def admin_command_error(
    interaction: discord.Interaction,
    error: app_commands.AppCommandError,
) -> None:
    if isinstance(error, app_commands.MissingPermissions):
        message = (
            "このコマンドは「サーバー管理」権限を持つ人だけ使えます。"
        )
    else:
        log.error("コマンドエラー: %r", error)
        message = "コマンドの実行中にエラーが発生しました。"

    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


# =========================================================
# 起動
# =========================================================

def main() -> None:
    if not TOKEN:
        raise RuntimeError(
            "RenderのEnvironmentへ DISCORD_TOKEN を設定してください。"
        )

    start_health_server()
    bot.run(TOKEN, log_handler=None)


if __name__ == "__main__":
    main()
