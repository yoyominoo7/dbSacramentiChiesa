import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import asyncpg
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

DIRECTION_CHAT_ID = int(os.getenv("DIRECTION_CHAT_ID", "0"))
STAFF_CHAT_ID = int(os.getenv("STAFF_CHAT_ID", "0"))

PRIESTS_IDS = set(
    int(x) for x in os.getenv("PRIESTS_IDS", "").split(",") if x.strip()
)
DIRECTION_IDS = set(
    int(x) for x in os.getenv("DIRECTION_IDS", "").split(",") if x.strip()
)

SACRAMENTS = [
    "battesimo",
    "cammino dell abisso",
    "rivelazione divina",
    "confessione",
    "unzione",
    "matrimonio",
    "divorzio",
]
MARRIAGE_DIVORCE = {"matrimonio", "divorzio"}

REG_NICK, REG_SACRAMENTS, REG_NOTES, REG_CONFIRM = range(4)
LIST_MAIN, LIST_BY_PRIEST, LIST_BY_ID, LIST_BY_FAITHFUL = range(10, 14)

SESSIONS: Dict[int, Dict[str, Any]] = {}


# ---------- DB UTILS ----------

async def get_db_pool():
    if not hasattr(get_db_pool, "pool"):
        get_db_pool.pool = await asyncpg.create_pool(DATABASE_URL)
    return get_db_pool.pool


async def upsert_priest(user):
    """Salva o aggiorna username e nome del sacerdote."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO priests (telegram_id, username, display_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (telegram_id)
            DO UPDATE SET username = EXCLUDED.username, display_name = EXCLUDED.display_name
            """,
            user.id,
            user.username,
            user.full_name,
        )


async def get_priest_name(telegram_id: int) -> str:
    """Restituisce @username oppure display_name se non esiste username."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT username, display_name FROM priests WHERE telegram_id = $1",
            telegram_id,
        )
    if not row:
        return f"ID {telegram_id}"
    if row["username"]:
        return f"@{row['username']}"
    return row["display_name"]


async def save_sacrament(
    faithful_nickname: str,
    sacraments: List[str],
    notes: str | None,
    priest_id: int,
    priest_username: str | None,
    direction_chat_id: int | None,
    direction_message_id: int | None,
):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO sacraments
            (faithful_nickname, sacraments, notes, priest_telegram_id, priest_username, direction_chat_id, direction_message_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            faithful_nickname,
            sacraments,
            notes,
            priest_id,
            priest_username,
            direction_chat_id,
            direction_message_id,
        )


async def fetch_sacraments_by_priest(priest_id: int, offset: int, limit: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT * FROM sacraments
            WHERE priest_telegram_id = $1
            ORDER BY created_at DESC
            OFFSET $2 LIMIT $3
            """,
            priest_id,
            offset,
            limit,
        )


async def count_sacraments_by_priest(priest_id: int) -> int:
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS c FROM sacraments WHERE priest_telegram_id = $1",
            priest_id,
        )
    return row["c"] if row else 0


async def fetch_sacrament_by_id(sacrament_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM sacraments WHERE id = $1",
            sacrament_id,
        )


async def fetch_sacraments_by_faithful(faithful: str, offset: int, limit: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT * FROM sacraments
            WHERE LOWER(faithful_nickname) LIKE LOWER($1)
            ORDER BY created_at DESC
            OFFSET $2 LIMIT $3
            """,
            f"%{faithful}%",
            offset,
            limit,
        )


async def count_sacraments_by_faithful(faithful: str) -> int:
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT COUNT(*) AS c FROM sacraments
            WHERE LOWER(faithful_nickname) LIKE LOWER($1)
            """,
            f"%{faithful}%",
        )
    return row["c"] if row else 0


async def fetch_all_priests_from_sacraments():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT DISTINCT priest_telegram_id, priest_username
            FROM sacraments
            ORDER BY priest_username NULLS LAST
            """
        )


async def fetch_weekly_report(start: datetime, end: datetime):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT *
            FROM sacraments
            WHERE created_at >= $1 AND created_at < $2
            """,
            start,
            end,
        )


# ---------- HELPERS ----------

def is_priest(user_id: int) -> bool:
    return user_id in PRIESTS_IDS


def is_direction(user_id: int) -> bool:
    return user_id in DIRECTION_IDS


def format_sacrament_record(row) -> str:
    created_at: datetime = row["created_at"]
    created_at_str = created_at.astimezone(timezone.utc).strftime("%d:%m:%Y %H:%M")
    sacrs = ", ".join(row["sacraments"])
    notes = row["notes"] or "Nessuna nota."
    priest_username = row["priest_username"] or f"id {row['priest_telegram_id']}"
    return (
        f"✝️ ID registrazione: {row['id']}\n"
        f"👤 Fedele: {row['faithful_nickname']}\n"
        f"📜 Sacramenti: {sacrs}\n"
        f"🕯 Note: {notes}\n"
        f"📅 Data: {created_at_str}\n"
        f"🙏 Registrato da: {priest_username}"
    )


def sacrament_keyboard(selected: List[str]) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for s in SACRAMENTS:
        if selected:
            if selected[0] in MARRIAGE_DIVORCE and s != selected[0]:
                continue
            if selected[0] not in MARRIAGE_DIVORCE and s in MARRIAGE_DIVORCE:
                continue
        label = f"{'✅ ' if s in selected else ''}{s}"
        row.append(InlineKeyboardButton(label, callback_data=f"sacr:{s}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("✅ Fine", callback_data="sacr_fine")])
    return InlineKeyboardMarkup(buttons)


def list_pagination_keyboard(base_cb: str, page: int, total_pages: int):
    buttons = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Indietro", callback_data=f"{base_cb}:{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Avanti ➡️", callback_data=f"{base_cb}:{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append(
        [InlineKeyboardButton("🏛 Torna al pannello principale", callback_data="list_main")]
    )
    return InlineKeyboardMarkup(buttons)

# ---------- /registra ----------

async def registra_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_priest(user.id):
        return ConversationHandler.END
    if update.effective_chat.type != "private":
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    text = (
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
        "🕯 Iniziamo la registrazione di un sacramento.\n"
        "Scrivi il nickname Minecraft del fedele che desideri registrare."
    )
    msg = await update.effective_chat.send_message(text)
    context.user_data["reg_msg_id"] = msg.message_id
    return REG_NICK


async def registra_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_priest(user.id):
        return ConversationHandler.END

    nickname = update.message.text.strip()
    context.user_data["reg_nick"] = nickname
    context.user_data["reg_sacraments"] = []

    try:
        await update.message.delete()
    except Exception:
        pass

    bot = context.bot
    chat_id = update.effective_chat.id
    reg_msg_id = context.user_data.get("reg_msg_id")

    text = (
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
        f"👤 Fedele: {nickname}\n\n"
        "✝️ Seleziona i sacramenti da registrare.\n"
        "Quando hai terminato, premi «Fine»."
    )
    kb = sacrament_keyboard([])

    if reg_msg_id:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=reg_msg_id,
            text=text,
            reply_markup=kb,
        )
    else:
        msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb,
        )
        context.user_data["reg_msg_id"] = msg.message_id

    return REG_SACRAMENTS


async def registra_sacrament_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user

    if not is_priest(user.id):
        return ConversationHandler.END

    data = query.data

    # --- Fine selezione sacramenti ---
    if data == "sacr_fine":
        selected = context.user_data.get("reg_sacraments", [])
        if not selected:
            await query.answer("Seleziona almeno un sacramento.", show_alert=True)
            return REG_SACRAMENTS

        # Matrimonio/divorzio → chiedi note
        if selected[0] in MARRIAGE_DIVORCE:
            text = (
                "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
                "🕯 Aggiungi eventuali note per questa registrazione.\n"
                "Se non ci sono note, scrivi «no»."
            )
            await context.bot.edit_message_text(
                chat_id=query.message.chat_id,
                message_id=query.message.message_id,
                text=text,
            )
            return REG_NOTES

        # Nessuna nota → vai al riepilogo
        context.user_data["reg_notes"] = None
        await send_registra_summary(update, context)
        return REG_CONFIRM

    # --- Selezione sacramenti ---
    _, sacr = data.split(":", 1)
    selected = context.user_data.get("reg_sacraments", [])

    # Toggle selezione
    if sacr in selected:
        selected.remove(sacr)
    else:
        # Regole matrimonio/divorzio
        if not selected and sacr in MARRIAGE_DIVORCE:
            selected.append(sacr)
        elif not selected:
            selected.append(sacr)
        else:
            if selected[0] in MARRIAGE_DIVORCE:
                await query.answer(
                    "Il matrimonio o il divorzio devono essere registrati da soli.",
                    show_alert=True,
                )
                return REG_SACRAMENTS
            selected.append(sacr)

    context.user_data["reg_sacraments"] = selected

    nickname = context.user_data.get("reg_nick", "Sconosciuto")
    text = (
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
        f"👤 Fedele: {nickname}\n\n"
        "✝️ Seleziona i sacramenti da registrare.\n"
        "Quando hai terminato, premi «Fine»."
    )

    kb = sacrament_keyboard(selected)

    await context.bot.edit_message_text(
        chat_id=query.message.chat_id,
        message_id=query.message.message_id,
        text=text,
        reply_markup=kb,
    )

    return REG_SACRAMENTS


async def registra_notes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_priest(user.id):
        return ConversationHandler.END

    notes = update.message.text.strip()
    if notes.lower() == "no":
        notes = None

    context.user_data["reg_notes"] = notes

    try:
        await update.message.delete()
    except Exception:
        pass

    await send_registra_summary(update, context)
    return REG_CONFIRM


async def send_registra_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    nickname = context.user_data.get("reg_nick", "Sconosciuto")
    sacrs = context.user_data.get("reg_sacraments", [])
    notes = context.user_data.get("reg_notes")

    sacrs_str = ", ".join(sacrs) if sacrs else "Nessuno"
    notes_str = notes if notes else "Nessuna nota."

    text = (
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
        "📜 Resoconto della registrazione:\n\n"
        f"👤 Fedele: {nickname}\n"
        f"✝️ Sacramenti: {sacrs_str}\n"
        f"🕯 Note: {notes_str}\n\n"
        "Confermi questa registrazione?"
    )

    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Conferma", callback_data="reg_conf_yes"),
                InlineKeyboardButton("❌ Annulla", callback_data="reg_conf_no"),
            ]
        ]
    )

    reg_msg_id = context.user_data.get("reg_msg_id")
    chat_id = update.effective_chat.id

    if update.callback_query:
        await context.bot.edit_message_text(
            chat_id=update.callback_query.message.chat_id,
            message_id=update.callback_query.message.message_id,
            text=text,
            reply_markup=kb,
        )
    elif reg_msg_id:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=reg_msg_id,
            text=text,
            reply_markup=kb,
        )
    else:
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb,
        )
        context.user_data["reg_msg_id"] = msg.message_id


async def registra_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user

    if not is_priest(user.id):
        return ConversationHandler.END

    data = query.data

    # --- Annulla ---
    if data == "reg_conf_no":
        await context.bot.edit_message_text(
            chat_id=query.message.chat_id,
            message_id=query.message.message_id,
            text="🕯 La registrazione è stata annullata. Nessun sacramento è stato registrato.",
        )
        context.user_data.clear()
        return ConversationHandler.END

    # --- Conferma ---
    nickname = context.user_data.get("reg_nick", "Sconosciuto")
    sacrs = context.user_data.get("reg_sacraments", [])
    notes = context.user_data.get("reg_notes")
    priest_username = user.username

    sacrs_str = ", ".join(sacrs) if sacrs else "Nessuno"
    notes_str = notes if notes else "Nessuna nota."

    direction_text = (
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
        "📜 Nuova registrazione di sacramento\n\n"
        f"👤 Fedele: {nickname}\n"
        f"✝️ Sacramenti: {sacrs_str}\n"
        f"🕯 Note: {notes_str}\n"
        f"🙏 Registrato da: @{priest_username}" if priest_username else ""
    )

    direction_msg = None
    if DIRECTION_CHAT_ID != 0:
        direction_msg = await context.bot.send_message(
            chat_id=DIRECTION_CHAT_ID,
            text=direction_text
        )

    await save_sacrament(
        faithful_nickname=nickname,
        sacraments=sacrs,
        notes=notes,
        priest_id=user.id,
        priest_username=user.username,
        direction_chat_id=DIRECTION_CHAT_ID if DIRECTION_CHAT_ID != 0 else None,
        direction_message_id=direction_msg.message_id if direction_msg else None,
    )

    await context.bot.edit_message_text(
        chat_id=query.message.chat_id,
        message_id=query.message.message_id,
        text="🕯 La registrazione è stata confermata e inviata alla Direzione.",
    )

    context.user_data.clear()
    return ConversationHandler.END

# ---------- /sessione ----------

async def sessione_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    # Solo sacerdoti e solo nella chat STAFF
    if not is_priest(user.id):
        return
    if chat.id != STAFF_CHAT_ID:
        return

    # 🔥 CONTROLLO NUOVO: impedisce sessioni multiple
    if any(SESSIONS.values()):
        await update.message.reply_text(
            "🕯 Una sessione sacerdotale è già attiva.\n"
            "Non è possibile avviarne un’altra finché quella corrente non termina."
        )
        return

    # Salva/aggiorna username del sacerdote
    await upsert_priest(user)

    text = (
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
        "🕯 Apertura sessione turni sacerdotali.\n"
        "Per i prossimi 2 minuti puoi unirti o abbandonare la sessione.\n\n"
        "🙏 Sacerdoti in sessione:\n"
        "— Nessuno per ora."
    )
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🙏 Unisciti alla sessione", callback_data="sess_join"),
                InlineKeyboardButton("🚪 Abbandona la sessione", callback_data="sess_leave"),
            ]
        ]
    )
    msg = await chat.send_message(text, reply_markup=kb)

    SESSIONS[msg.message_id] = {
        "chat_id": chat.id,
        "message_id": msg.message_id,
        "priests": set(),
        "in_turn": set(),
        "waiting": set(),
        "phase": "join",
    }

    context.job_queue.run_once(
        sessione_close_join_phase,
        when=120,
        data={"message_id": msg.message_id},
        name=f"session_join_{msg.message_id}",
    )


async def sessione_close_join_phase(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    message_id = data["message_id"]
    session = SESSIONS.get(message_id)
    if not session:
        return

    chat_id = session["chat_id"]
    priests = session["priests"]
    bot = context.bot

    if len(priests) < 3:
        text = (
            "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
            "🕯 La sessione non può iniziare.\n"
            "Non è stato raggiunto il numero minimo di 3 sacerdoti."
        )
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
        )
        del SESSIONS[message_id]
        return

    import random

    priests_list = list(priests)
    random.shuffle(priests_list)
    in_turn = set(priests_list[:2])
    waiting = set(priests_list[2:])

    session["in_turn"] = in_turn
    session["waiting"] = waiting
    session["phase"] = "running"

    await update_session_message(bot, session)

    context.job_queue.run_repeating(
        sessione_next_turn,
        interval=30 * 60,
        first=30 * 60,
        data={"message_id": message_id},
        name=f"session_turn_{message_id}",
    )


async def update_session_message(bot, session: Dict[str, Any]):
    chat_id = session["chat_id"]
    message_id = session["message_id"]

    # Recupera i nomi veri dal DB
    async def fmt(uid):
        return await get_priest_name(uid)

    in_turn_list = "\n".join([f"• Sacerdote {await fmt(uid)}" for uid in session["in_turn"]]) or "— Nessuno"
    waiting_list = "\n".join([f"• Sacerdote {await fmt(uid)}" for uid in session["waiting"]]) or "— Nessuno"
    all_list = "\n".join([f"• Sacerdote {await fmt(uid)}" for uid in session["priests"]]) or "— Nessuno"

    text = (
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
        "🕯 Sessione turni sacerdotali attiva.\n\n"
        "⏱ Turno corrente (30 minuti):\n"
        f"{in_turn_list}\n\n"
        "📜 In attesa di entrare in turno:\n"
        f"{waiting_list}\n\n"
        "🙏 Sacerdoti in sessione:\n"
        f"{all_list}"
    )

    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🙏 Unisciti ai turni", callback_data="turn_join"),
                InlineKeyboardButton("🚪 Abbandona i turni", callback_data="turn_leave"),
            ]
        ]
    )

    await bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        reply_markup=kb,
    )


async def sessione_join_leave_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user

    # Aggiorna @ del sacerdote
    await upsert_priest(user)

    message_id = query.message.message_id
    session = SESSIONS.get(message_id)
    if not session:
        await query.answer("Questa sessione non è più attiva.", show_alert=True)
        return

    if query.data == "sess_join":
        session["priests"].add(user.id)
        await query.answer("Ti sei unito alla sessione.")
    elif query.data == "sess_leave":
        session["priests"].discard(user.id)
        await query.answer("Hai abbandonato la sessione.")

    if session["phase"] == "join":
        priests = session["priests"]
        async def fmt(uid): return await get_priest_name(uid)
        priests_list = "\n".join([f"• Sacerdote {await fmt(uid)}" for uid in priests]) or "— Nessuno"

        text = (
            "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
            "🕯 Apertura sessione turni sacerdotali.\n"
            "Per i prossimi 2 minuti puoi unirti o abbandonare la sessione.\n\n"
            "🙏 Sacerdoti in sessione:\n"
            f"{priests_list}"
        )
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🙏 Unisciti alla sessione", callback_data="sess_join"),
                    InlineKeyboardButton("🚪 Abbandona la sessione", callback_data="sess_leave"),
                ]
            ]
        )
        await context.bot.edit_message_text(
            chat_id=query.message.chat_id,
            message_id=query.message.message_id,
            text=text,
            reply_markup=kb,
        )
    else:
        if query.data == "turn_join":
            session["priests"].add(user.id)
            if user.id not in session["in_turn"]:
                session["waiting"].add(user.id)
                session["priests"].add(user.id)
            await query.answer("Ti sei messo in lista per i turni.")

        elif query.data == "turn_leave":
            if user.id in session["in_turn"]:
                session["in_turn"].discard(user.id)
                session["priests"].discard(user.id)
                if session["waiting"]:
                    new_priest = session["waiting"].pop()
                    session["in_turn"].add(new_priest)
            if user.id in session["waiting"]:
                session["waiting"].discard(user.id)
                session["priests"].discard(user.id)
            await query.answer("Hai lasciato i turni.")

        if len(session["priests"]) < 3:
            await context.bot.edit_message_text(
                chat_id=query.message.chat_id,
                message_id=query.message.message_id,
                text="🕯 La sessione è terminata: non ci sono più almeno 3 sacerdoti.",
            )
            del SESSIONS[message_id]
            return

        await update_session_message(context.bot, session)


async def sessione_next_turn(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    message_id = data["message_id"]
    session = SESSIONS.get(message_id)
    if not session:
        context.job.schedule_removal()
        return

    bot = context.bot

    if len(session["priests"]) < 3:
        await bot.edit_message_text(
            chat_id=session["chat_id"],
            message_id=session["message_id"],
            text=(
                "🕯 La sessione è terminata: non ci sono più almeno 3 sacerdoti.\n"
                "I messaggi relativi alla sessione verranno rimossi."
            ),
        )
        try:
            await bot.delete_message(
                chat_id=session["chat_id"], message_id=session["message_id"]
            )
        except Exception:
            pass
        del SESSIONS[message_id]
        context.job.schedule_removal()
        return

    import random

    priests = list(session["priests"])
    random.shuffle(priests)

    prev_in_turn = session["in_turn"]
    candidates = [p for p in priests if p not in prev_in_turn]
    if len(candidates) < 2:
        candidates = priests

    new_in_turn = set(candidates[:2])
    waiting = set(priests) - new_in_turn

    session["in_turn"] = new_in_turn
    session["waiting"] = waiting

    await update_session_message(bot, session)


# ---------- /lista_sacramenti ----------

async def lista_sacramenti_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    if not is_direction(user.id):
        return ConversationHandler.END
    if chat.id != DIRECTION_CHAT_ID:
        return ConversationHandler.END

    text = (
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
        "📜 Pannello registrazioni sacramenti.\n\n"
        "Scegli una modalità di consultazione:"
    )
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🙏 Sacerdote", callback_data="list_by_priest"),
            ],
            [
                InlineKeyboardButton("🔎 Cerca per ID", callback_data="list_by_id"),
            ],
            [
                InlineKeyboardButton("👤 Cerca per fedele", callback_data="list_by_faithful"),
            ],
        ]
    )
    await update.message.reply_text(text, reply_markup=kb)
    return LIST_MAIN


async def lista_main_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "list_main":
        text = (
            "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
            "📜 Pannello registrazioni sacramenti.\n\n"
            "Scegli una modalità di consultazione:"
        )
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🙏 Sacerdote", callback_data="list_by_priest"),
                ],
                [
                    InlineKeyboardButton("🔎 Cerca per ID", callback_data="list_by_id"),
                ],
                [
                    InlineKeyboardButton("👤 Cerca per fedele", callback_data="list_by_faithful"),
                ],
            ]
        )
        await context.bot.edit_message_text(
            chat_id=query.message.chat_id,
            message_id=query.message.message_id,
            text=text,
            reply_markup=kb,
        )
        return LIST_MAIN

    if data == "list_by_priest":
        priests = await fetch_all_priests_from_sacraments()
        buttons = []
        row = []
        for p in priests:
            username = p["priest_username"] or f"id {p['priest_telegram_id']}"
            row.append(
                InlineKeyboardButton(
                    username, callback_data=f"list_priest:{p['priest_telegram_id']}:0"
                )
            )
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append(
            [InlineKeyboardButton("🏛 Torna al pannello principale", callback_data="list_main")]
        )
        kb = InlineKeyboardMarkup(buttons)
        text = (
            "🕯 Seleziona un sacerdote per visualizzare le sue registrazioni.\n"
            "Le registrazioni saranno ordinate dalla più recente alla più antica."
        )
        await context.bot.edit_message_text(
            chat_id=query.message.chat_id,
            message_id=query.message.message_id,
            text=text,
            reply_markup=kb,
        )
        return LIST_BY_PRIEST

    if data == "list_by_id":
        text = (
            "🕯 Invia l'ID della registrazione che desideri consultare.\n"
            "Puoi tornare indietro in qualsiasi momento."
        )
        await context.bot.edit_message_text(
            chat_id=query.message.chat_id,
            message_id=query.message.message_id,
            text=text,
        )
        return LIST_BY_ID

    if data == "list_by_faithful":
        text = (
            "🕯 Invia il nome del fedele che desideri cercare.\n"
            "Le registrazioni saranno mostrate in ordine dalla più recente."
        )
        await context.bot.edit_message_text(
            chat_id=query.message.chat_id,
            message_id=query.message.message_id,
            text=text,
        )
        return LIST_BY_FAITHFUL


async def lista_priest_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    _, priest_id_str, page_str = data.split(":")
    priest_id = int(priest_id_str)
    page = int(page_str)

    per_page = 7
    total = await count_sacraments_by_priest(priest_id)
    total_pages = max(1, (total + per_page - 1) // per_page)
    offset = page * per_page

    rows = await fetch_sacraments_by_priest(priest_id, offset, per_page)
    if not rows:
        text = (
            "🕯 Nessuna registrazione trovata per questo sacerdote.\n"
            "Puoi tornare al pannello principale."
        )
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏛 Torna al pannello principale", callback_data="list_main")]]
        )
        await context.bot.edit_message_text(
            chat_id=query.message.chat_id,
            message_id=query.message.message_id,
            text=text,
            reply_markup=kb,
        )
        return LIST_BY_PRIEST

    lines = []
    for r in rows:
        lines.append(format_sacrament_record(r))
        lines.append("— — —")

    text = (
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
        f"📜 Registrazioni del sacerdote (pagina {page+1}/{total_pages}):\n\n"
        + "\n".join(lines)
    )

    kb = list_pagination_keyboard(f"list_priest:{priest_id}", page, total_pages)
    await context.bot.edit_message_text(
        chat_id=query.message.chat_id,
        message_id=query.message.message_id,
        text=text,
        reply_markup=kb,
    )
    return LIST_BY_PRIEST


async def lista_by_id_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        sac_id = int(text)
    except ValueError:
        await update.message.reply_text("🕯 L'ID deve essere un numero. Riprova.")
        return ConversationHandler.END

    row = await fetch_sacrament_by_id(sac_id)
    if not row:
        await update.message.reply_text(
            "🕯 Nessuna registrazione trovata con questo ID."
        )
        return ConversationHandler.END

    msg = format_sacrament_record(row)
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🏛 Torna al pannello principale", callback_data="list_main")]]
    )
    await update.message.reply_text(msg, reply_markup=kb)
    return LIST_MAIN


async def lista_by_faithful_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    faithful = update.message.text.strip()
    context.user_data["list_faithful"] = faithful
    page = 0
    per_page = 7
    total = await count_sacraments_by_faithful(faithful)
    total_pages = max(1, (total + per_page - 1) // per_page)
    offset = page * per_page

    rows = await fetch_sacraments_by_faithful(faithful, offset, per_page)
    if not rows:
        await update.message.reply_text(
            "🕯 Nessuna registrazione trovata per questo fedele."
        )
        return ConversationHandler.END

    lines = []
    for r in rows:
        lines.append(format_sacrament_record(r))
        lines.append("— — —")

    text = (
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
        f"📜 Registrazioni per il fedele «{faithful}» (pagina {page+1}/{total_pages}):\n\n"
        + "\n".join(lines)
    )

    kb = list_pagination_keyboard("list_faithful", page, total_pages)
    await update.message.reply_text(text, reply_markup=kb)
    return LIST_BY_FAITHFUL

async def lista_faithful_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    _, page_str = data.split(":")
    page = int(page_str)

    faithful = context.user_data.get("list_faithful", "")
    if not faithful:
        await query.answer("Nessun fedele in memoria.", show_alert=True)
        return LIST_BY_FAITHFUL

    per_page = 7
    total = await count_sacraments_by_faithful(faithful)
    total_pages = max(1, (total + per_page - 1) // per_page)
    offset = page * per_page

    rows = await fetch_sacraments_by_faithful(faithful, offset, per_page)
    if not rows:
        await query.answer("Nessuna registrazione in questa pagina.", show_alert=True)
        return LIST_BY_FAITHFUL

    lines = []
    for r in rows:
        lines.append(format_sacrament_record(r))
        lines.append("— — —")

    text = (
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
        f"📜 Registrazioni per il fedele «{faithful}» (pagina {page+1}/{total_pages}):\n\n"
        + "\n".join(lines)
    )

    kb = list_pagination_keyboard("list_faithful", page, total_pages)
    await context.bot.edit_message_text(
        chat_id=query.message.chat_id,
        message_id=query.message.message_id,
        text=text,
        reply_markup=kb,
    )
    return LIST_BY_FAITHFUL
# ---------- REPORT SETTIMANALE ----------

async def weekly_report_job(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(timezone.utc)
    weekday = now.weekday()

    this_monday = datetime(
        year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc
    ) - timedelta(
        days=weekday,
        hours=now.hour,
        minutes=now.minute,
        seconds=now.second,
        microseconds=now.microsecond,
    )

    last_monday = this_monday - timedelta(days=7)
    start = last_monday
    end = this_monday

    rows = await fetch_weekly_report(start, end)
    if not rows:
        text = (
            "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n"
            "📊 Report settimanale\n"
            f"🗓 Periodo: {start.date()} ➝ {end.date()}\n"
            "✝️ Totale sacramenti completati: 0\n\n"
            "Nessuna registrazione in questa settimana."
        )
        if DIRECTION_CHAT_ID != 0:
            await context.bot.send_message(DIRECTION_CHAT_ID, text)
        return

    total = len(rows)
    priest_stats: Dict[str, Dict[str, int]] = {}
    sacrament_totals: Dict[str, int] = {}

    for r in rows:
        priest_username = r["priest_username"] or f"id {r['priest_telegram_id']}"
        sacrs = r["sacraments"]

        if priest_username not in priest_stats:
            priest_stats[priest_username] = {}

        for s in sacrs:
            priest_stats[priest_username][s] = priest_stats[priest_username].get(s, 0) + 1
            sacrament_totals[s] = sacrament_totals.get(s, 0) + 1

    priest_order = sorted(
        priest_stats.items(),
        key=lambda kv: sum(kv[1].values()),
        reverse=True,
    )

    lines = []
    lines.append("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n")
    lines.append("📊 Report settimanale")
    lines.append(f"🗓 Periodo: {start.date()} ➝ {end.date()}")
    lines.append(f"✝️ Totale sacramenti completati: {total}\n")
    lines.append("🏆 Classifica sacerdoti:")

    for priest_username, sacrs_dict in priest_order:
        total_priest = sum(sacrs_dict.values())
        detail_parts = []
        for s, c in sacrs_dict.items():
            if c == 1:
                detail_parts.append(f"{s}")
            else:
                detail_parts.append(f"{s} ({c} volte)")
        detail_str = ", ".join(detail_parts)
        lines.append(f"- 🙏 Sacerdote {priest_username}: {total_priest} ➝ {detail_str}")

    lines.append("\n✝️ Dettaglio per sacramento (totale):")
    for s, c in sacrament_totals.items():
        lines.append(f"- {s}: {c}")

    lines.append("\n📌 Prenotazioni ancora aperte: 0")

    text = "\n".join(lines)
    if DIRECTION_CHAT_ID != 0:
        await context.bot.send_message(DIRECTION_CHAT_ID, text)

# ---------- MAIN ----------

def main():
    application: Application = (
        ApplicationBuilder()
        .token(TOKEN)
        .build()
    )

    # --- Conversazione /registra ---
    registra_conv = ConversationHandler(
        entry_points=[CommandHandler("registra", registra_start)],
        states={
            REG_NICK: [MessageHandler(filters.TEXT & ~filters.COMMAND, registra_nick)],
            REG_SACRAMENTS: [
                CallbackQueryHandler(registra_sacrament_callback, pattern=r"^sacr")
            ],
            REG_NOTES: [MessageHandler(filters.TEXT & ~filters.COMMAND, registra_notes)],
            REG_CONFIRM: [
                CallbackQueryHandler(registra_confirm_callback, pattern=r"^reg_conf_")
            ],
        },
        fallbacks=[],
        name="registra_conv",
        persistent=False,
    )

    # --- /sessione ---
    application.add_handler(CommandHandler("sessione", sessione_start))
    application.add_handler(
        CallbackQueryHandler(
            sessione_join_leave_callback,
            pattern=r"^(sess_join|sess_leave|turn_join|turn_leave)$",
        )
    )

    # --- Conversazione /lista_sacramenti ---
    lista_conv = ConversationHandler(
        entry_points=[CommandHandler("lista_sacramenti", lista_sacramenti_start)],
        states={
            LIST_MAIN: [
                CallbackQueryHandler(lista_main_callback, pattern=r"^list_"),
            ],
            LIST_BY_PRIEST: [
                CallbackQueryHandler(lista_priest_callback, pattern=r"^list_priest:"),
                CallbackQueryHandler(lista_main_callback, pattern=r"^list_main$"),
            ],
            LIST_BY_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, lista_by_id_message),
                CallbackQueryHandler(lista_main_callback, pattern=r"^list_main$"),
            ],
            LIST_BY_FAITHFUL: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, lista_by_faithful_message
                ),
                CallbackQueryHandler(lista_faithful_pagination, pattern=r"^list_faithful:"),
                CallbackQueryHandler(lista_main_callback, pattern=r"^list_main$"),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(lista_main_callback, pattern=r"^list_main$"),
        ],
        name="lista_sacramenti_conv",
        persistent=False,
    )

    application.add_handler(registra_conv)
    application.add_handler(lista_conv)

    # ---------- JOB SETTIMANALE ----------
    now = datetime.now(timezone.utc)
    weekday = now.weekday()
    days_until_monday = (7 - weekday) % 7

    next_monday = datetime(
        year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc
    ) + timedelta(days=days_until_monday)

    next_monday_midnight = next_monday.replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    if next_monday_midnight <= now:
        next_monday_midnight += timedelta(days=7)

    application.job_queue.run_repeating(
        weekly_report_job,
        interval=7 * 24 * 60 * 60,
        first=next_monday_midnight - now,
        name="weekly_report",
    )

    # ---------- WEBHOOK PER RENDER ----------
    port = int(os.environ.get("PORT", 10000))
    webhook_url = os.environ.get("WEBHOOK_URL")

    application.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path=TOKEN,
        webhook_url=f"{webhook_url}/{TOKEN}",
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
