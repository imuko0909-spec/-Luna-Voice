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

import aiohttp
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
VOICEVOX_ENGINE_URL = os.getenv("VOICEVOX_ENGINE_URL", "").strip().rstrip("/")
DEFAULT_STYLE_ID = int(os.getenv("DEFAULT_STYLE_ID", "3"))
FALLBACK_GTTS = os.getenv("FALLBACK_GTTS", "true").lower() in {"1","true","yes","on"}

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
                    read_names INTEGER NOT NULL DEFAULT 1,
                    style_id INTEGER NOT NULL DEFAULT 3,
                    speed REAL NOT NULL DEFAULT 1.0,
                    pitch REAL NOT NULL DEFAULT 0.0,
                    intonation REAL NOT NULL DEFAULT 1.0
                );
                CREATE TABLE IF NOT EXISTS user_voice(
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    style_id INTEGER NOT NULL,
                    PRIMARY KEY(guild_id,user_id)
                );
                CREATE TABLE IF NOT EXISTS dictionary(
                    guild_id INTEGER NOT NULL,
                    word TEXT NOT NULL,
                    reading TEXT NOT NULL,
                    PRIMARY KEY(guild_id, word)
                );
                """
            )
            # 旧DBからの安全な追加
            for sql in (
                "ALTER TABLE settings ADD COLUMN style_id INTEGER NOT NULL DEFAULT 3",
                "ALTER TABLE settings ADD COLUMN speed REAL NOT NULL DEFAULT 1.0",
                "ALTER TABLE settings ADD COLUMN pitch REAL NOT NULL DEFAULT 0.0",
                "ALTER TABLE settings ADD COLUMN intonation REAL NOT NULL DEFAULT 1.0",
            ):
                try:
                    con.execute(sql)
                except sqlite3.OperationalError:
                    pass

    def connect(self):
        return sqlite3.connect(self.path, timeout=30)

    def settings(self, guild_id: int) -> dict:
        with self.lock, self.connect() as con:
            con.execute("INSERT OR IGNORE INTO settings(guild_id,style_id) VALUES(?,?)", (guild_id, DEFAULT_STYLE_ID))
            row = con.execute("SELECT volume,read_names,style_id,speed,pitch,intonation FROM settings WHERE guild_id=?", (guild_id,)).fetchone()
        return {"volume":float(row[0]),"read_names":bool(row[1]),"style_id":int(row[2]),"speed":float(row[3]),"pitch":float(row[4]),"intonation":float(row[5])}

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


    def set_voice_setting(self, guild_id: int, column: str, value):
        if column not in {"style_id","speed","pitch","intonation"}:
            raise ValueError(column)
        self.settings(guild_id)
        with self.lock, self.connect() as con:
            con.execute(f"UPDATE settings SET {column}=? WHERE guild_id=?", (value,guild_id))

    def set_user_voice(self, guild_id: int, user_id: int, style_id: int):
        with self.lock, self.connect() as con:
            con.execute("INSERT INTO user_voice VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET style_id=excluded.style_id", (guild_id,user_id,style_id))

    def get_user_voice(self, guild_id: int, user_id: int):
        with self.lock, self.connect() as con:
            row=con.execute("SELECT style_id FROM user_voice WHERE guild_id=? AND user_id=?",(guild_id,user_id)).fetchone()
        return int(row[0]) if row else None

    def remove_user_voice(self, guild_id: int, user_id: int):
        with self.lock, self.connect() as con:
            return con.execute("DELETE FROM user_voice WHERE guild_id=? AND user_id=?",(guild_id,user_id)).rowcount>0


db = Database(DB_PATH)


class Session:
    def __init__(self):
        self.text_channel_id: Optional[int] = None
        self.queue: asyncio.Queue[tuple[str, float, int, dict]] = asyncio.Queue(maxsize=100)
        self.worker: Optional[asyncio.Task] = None
        self.auto_disconnect: Optional[asyncio.Task] = None


sessions: dict[int, Session] = defaultdict(Session)

VOICE_STYLES: dict[int,str] = {}
HTTP: Optional[aiohttp.ClientSession] = None

async def refresh_voices():
    global HTTP, VOICE_STYLES
    if not VOICEVOX_ENGINE_URL:
        return {}
    if HTTP is None or HTTP.closed:
        HTTP=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    try:
        async with HTTP.get(f"{VOICEVOX_ENGINE_URL}/speakers") as r:
            r.raise_for_status(); data=await r.json()
        VOICE_STYLES={int(st["id"]):f'{sp["name"]}（{st["name"]}）' for sp in data for st in sp.get("styles",[])}
        log.info("VOICEVOX voices=%s",len(VOICE_STYLES)); return VOICE_STYLES
    except Exception:
        log.exception("VOICEVOX speakers failed"); return VOICE_STYLES

async def make_voicevox(text, style_id, cfg):
    global HTTP
    if not VOICEVOX_ENGINE_URL: raise RuntimeError("VOICEVOX_ENGINE_URL未設定")
    if HTTP is None or HTTP.closed:
        HTTP=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    params={"text":text,"speaker":style_id}
    async with HTTP.post(f"{VOICEVOX_ENGINE_URL}/audio_query",params=params) as r:
        r.raise_for_status(); q=await r.json()
    q["speedScale"]=cfg["speed"]; q["pitchScale"]=cfg["pitch"]; q["intonationScale"]=cfg["intonation"]
    async with HTTP.post(f"{VOICEVOX_ENGINE_URL}/synthesis",params={"speaker":style_id},json=q) as r:
        r.raise_for_status(); audio=await r.read()
    filename=str(Path(tempfile.gettempdir())/f"voicevox_{uuid.uuid4().hex}.wav")
    await asyncio.to_thread(Path(filename).write_bytes,audio); return filename

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
        text, volume, style_id, cfg = await s.queue.get()
        filename = None
        try:
            guild = bot.get_guild(guild_id)
            voice = guild.voice_client if guild else None
            if not voice or not voice.is_connected():
                continue
            try:
                filename = await make_voicevox(text, style_id, cfg)
            except Exception:
                log.exception("VOICEVOX failed; fallback")
                if not FALLBACK_GTTS: raise
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
    cfg = db.settings(message.guild.id)
    volume, read_names = cfg["volume"], cfg["read_names"]
    if read_names and isinstance(message.author, discord.Member):
        text = f"{message.author.display_name}、{text}"
    try:
        sessions[message.guild.id].queue.put_nowait((text, volume))
    except asyncio.QueueFull:
        log.warning("queue full guild=%s", message.guild.id)


@bot.event
async def on_ready():
    log.info("ログイン完了: %s", bot.user)
    await refresh_voices()
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


async def voice_choices(interaction: discord.Interaction,current: str):
    if not VOICE_STYLES: await refresh_voices()
    key=current.lower(); out=[]
    for sid,name in VOICE_STYLES.items():
        label=f"{name}｜ID:{sid}"
        if not key or key in label.lower(): out.append(app_commands.Choice(name=label[:100],value=str(sid)))
        if len(out)>=25: break
    return out

@bot.tree.command(name="声",description="サーバー標準のVOICEVOX音声を変更")
@app_commands.describe(voice="ずんだもん等を検索")
@app_commands.autocomplete(voice=voice_choices)
@app_commands.checks.has_permissions(manage_guild=True)
async def set_voice(interaction: discord.Interaction,voice: str):
    if not interaction.guild: return
    try: sid=int(voice)
    except ValueError: return await interaction.response.send_message("候補から選んでください",ephemeral=True)
    db.set_voice_setting(interaction.guild.id,"style_id",sid)
    await interaction.response.send_message(f"🎤 標準音声を **{VOICE_STYLES.get(sid,sid)}** に変更しました")

@bot.tree.command(name="個人声",description="自分の投稿を読む声を変更")
@app_commands.describe(voice="自分用の声")
@app_commands.autocomplete(voice=voice_choices)
async def personal_voice(interaction: discord.Interaction,voice: str):
    if not interaction.guild: return
    try: sid=int(voice)
    except ValueError: return await interaction.response.send_message("候補から選んでください",ephemeral=True)
    db.set_user_voice(interaction.guild.id,interaction.user.id,sid)
    await interaction.response.send_message(f"🗣️ あなたの声を **{VOICE_STYLES.get(sid,sid)}** にしました",ephemeral=True)

@bot.tree.command(name="個人声解除",description="自分専用の声を解除")
async def personal_voice_remove(interaction: discord.Interaction):
    if interaction.guild: db.remove_user_voice(interaction.guild.id,interaction.user.id)
    await interaction.response.send_message("個人声を解除しました",ephemeral=True)

@bot.tree.command(name="声一覧",description="VOICEVOXの声一覧を表示")
async def voice_list(interaction: discord.Interaction):
    await refresh_voices()
    text="🎤 **声一覧**\n"+"\n".join(f"`{i}` {n}" for i,n in list(VOICE_STYLES.items())[:45])
    await interaction.response.send_message(text[:1900] or "VOICEVOXへ接続できません",ephemeral=True)

@bot.tree.command(name="話速",description="話す速さ 0.5〜2.0")
@app_commands.checks.has_permissions(manage_guild=True)
async def speed_cmd(interaction: discord.Interaction,value: app_commands.Range[float,0.5,2.0]):
    db.set_voice_setting(interaction.guild.id,"speed",float(value)); await interaction.response.send_message(f"話速を{value}に変更",ephemeral=True)

@bot.tree.command(name="音程",description="声の高さ -0.15〜0.15")
@app_commands.checks.has_permissions(manage_guild=True)
async def pitch_cmd(interaction: discord.Interaction,value: app_commands.Range[float,-0.15,0.15]):
    db.set_voice_setting(interaction.guild.id,"pitch",float(value)); await interaction.response.send_message(f"音程を{value}に変更",ephemeral=True)

@bot.tree.command(name="抑揚",description="抑揚 0〜2")
@app_commands.checks.has_permissions(manage_guild=True)
async def intonation_cmd(interaction: discord.Interaction,value: app_commands.Range[float,0.0,2.0]):
    db.set_voice_setting(interaction.guild.id,"intonation",float(value)); await interaction.response.send_message(f"抑揚を{value}に変更",ephemeral=True)


if __name__ == "__main__":
    main()
