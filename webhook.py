import sqlite3, os, logging, sys
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes
)

# ── LOGGING ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
for _h in logging.root.handlers:
    if isinstance(_h, logging.StreamHandler) and not isinstance(_h, logging.FileHandler):
        try:
            _h.stream = open(sys.stdout.fileno(), mode='w',
                             encoding='utf-8', errors='replace', closefd=False)
        except Exception:
            pass
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════
# SINGLE-INSTANCE LOCK
# ═══════════════════════════════════════════════════════════════════════
LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.lock')

def acquire_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, 'r') as f:
                old_pid = int(f.read().strip())
            try:
                import psutil
                if psutil.pid_exists(old_pid):
                    log.error("Another instance running (PID %s). Kill: taskkill /PID %s /F", old_pid, old_pid)
                    sys.exit(1)
                else:
                    log.warning("Stale lock (PID %s dead) overwriting.", old_pid)
            except ImportError:
                log.warning("psutil not installed; overwriting lock.")
        except Exception:
            pass
    with open(LOCK_FILE, 'w') as f:
        f.write(str(os.getpid()))
    log.info("Lock acquired PID=%s", os.getpid())

def release_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '8135250878:AAH7lbH1xf8mXDdTOQT0XezKnxxauSU5Cg4')
ALERT_CHAT_ID      = os.getenv('ALERT_CHAT_ID',      '-5112676710')

FACTORY_DB   = os.getenv('FACTORY_DB',   r'E:\Factor\factData.db')
SOURCE_TABLE = os.getenv('SOURCE_TABLE', 'log_data')

FAILURE_THRESHOLD = float(os.getenv('FAILURE_THRESHOLD', '3.0'))
MIN_SAMPLES       = int(os.getenv('MIN_SAMPLES',         '5'))
TOP_N_LINES       = int(os.getenv('TOP_N_LINES',         '3'))
TOP_N_TESTCODES   = int(os.getenv('TOP_N_TESTCODES',     '3'))
POLL_INTERVAL_SEC = int(os.getenv('POLL_INTERVAL_SEC',   '600'))
SLA_HOURS         = int(os.getenv('SLA_HOURS',           '1'))
WINDOW_MINUTES    = int(os.getenv('WINDOW_MINUTES',      '240'))

MEDIA_DIR = os.getenv('MEDIA_DIR', os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'repair_media'))
os.makedirs(MEDIA_DIR, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════
# TIMESTAMP  — always local time (DB stores local IST)
# ═══════════════════════════════════════════════════════════════════════

def now_local() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def since_str(minutes: int) -> str:
    return (datetime.now() - timedelta(minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S')

# Normalised timestamp expression (handles ISO 'T', NULL, or split date+time columns)
TS_EXPR = "replace(COALESCE(NULLIF(TRIM(timestamp),''), date||' '||time), 'T', ' ')"

# ═══════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════

def get_db():
    conn = sqlite3.connect(FACTORY_DB, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn

def _col_exists(conn, table, col):
    return col in {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}

def _migrate_col(conn, table, col, typedef):
    if not _col_exists(conn, table, col):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")
            conn.commit()
            log.info("Migration: added %s.%s (%s)", table, col, typedef)
        except Exception as e:
            log.warning("Migration skip %s.%s: %s", table, col, e)

def init_db():
    conn = get_db()

    # ── failure_alerts ────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS failure_alerts (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            source_table      TEXT    NOT NULL DEFAULT 'log_data',
            line              TEXT    NOT NULL,
            station           TEXT    NOT NULL,
            line_failure_rate REAL    NOT NULL,
            total_tests       INTEGER NOT NULL,
            failed_tests      INTEGER NOT NULL,
            top_testcodes     TEXT,
            alert_sent_at     TEXT    NOT NULL,
            resolved          INTEGER NOT NULL DEFAULT 0,
            resolved_at       TEXT,
            resolution_mins   REAL,
            within_sla        INTEGER,
            telegram_msg_id   TEXT
        )
    """)
    conn.commit()

    # ── repair_actions ────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS repair_actions (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id            INTEGER,
            unit_id             TEXT,
            line                TEXT,
            station             TEXT,
            status              TEXT DEFAULT 'Open',
            failure_description TEXT,
            rca_text            TEXT,
            action_taken        TEXT,
            capa_text           TEXT,
            engineer_contact    TEXT,
            created_at          TEXT,
            updated_at          TEXT,
            closed_at           TEXT,
            FOREIGN KEY (alert_id) REFERENCES failure_alerts(id)
        )
    """)
    conn.commit()

    _migrate_col(conn, 'repair_actions', 'alert_id',            'INTEGER')
    _migrate_col(conn, 'repair_actions', 'line',                'TEXT')
    _migrate_col(conn, 'repair_actions', 'failure_description', 'TEXT')
    _migrate_col(conn, 'repair_actions', 'rca_text',            'TEXT')
    _migrate_col(conn, 'repair_actions', 'action_taken',        'TEXT')
    _migrate_col(conn, 'repair_actions', 'capa_text',           'TEXT')
    _migrate_col(conn, 'repair_actions', 'engineer_contact',    'TEXT')

    conn.execute("""
        UPDATE repair_actions
        SET alert_id = CAST(SUBSTR(unit_id, 7) AS INTEGER)
        WHERE alert_id IS NULL
          AND unit_id LIKE 'ALERT-%'
          AND CAST(SUBSTR(unit_id, 7) AS INTEGER) > 0
    """)
    backfilled = conn.total_changes
    if backfilled:
        log.info("Backfilled alert_id for %d repair_actions rows", backfilled)
    conn.commit()

    # ── repair_media ──────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS repair_media (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            repair_id   INTEGER,
            alert_id    INTEGER,
            filepath    TEXT    NOT NULL,
            caption     TEXT,
            uploaded_by TEXT,
            media_type  TEXT    DEFAULT 'image',
            uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    _migrate_col(conn, 'repair_media', 'alert_id',  'INTEGER')
    _migrate_col(conn, 'repair_media', 'repair_id', 'INTEGER')
    _migrate_col(conn, 'repair_media', 'media_type','TEXT')
    conn.commit()
    conn.close()
    log.info("DB ready: %s | table: %s | media: %s", FACTORY_DB, SOURCE_TABLE, MEDIA_DIR)

# ═══════════════════════════════════════════════════════════════════════
# QUERIES  — all use deduplicated track_id (latest status per unit)
# ═══════════════════════════════════════════════════════════════════════

# ---------------------------------------------------------------------------
# Deduplicated CTE: for a given window, keep only the LATEST row per track_id
# per line (so each physical unit counts once).
# If the table has no track_id column we fall back to counting raw rows.
# ---------------------------------------------------------------------------
def _has_track_id(conn) -> bool:
    return _col_exists(conn, SOURCE_TABLE, 'track_id')

def _dedup_cte(conn, since: str) -> tuple[str, list]:
    """
    Returns (cte_sql, params) for a WITH deduplicated AS (...) block.
    The CTE produces columns: line, station, status, failed_testcode, process
    de-duplicated by track_id (latest timestamp per track_id).
    Falls back to raw rows when track_id column is absent.
    """
    if _has_track_id(conn):
        sql = f"""
            WITH deduplicated AS (
                SELECT line, station, status, failed_testcode, process
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY track_id
                               ORDER BY {TS_EXPR} DESC
                           ) AS _rn
                    FROM {SOURCE_TABLE}
                    WHERE {TS_EXPR} >= ?
                      AND track_id IS NOT NULL
                      AND TRIM(track_id) != ''
                )
                WHERE _rn = 1
                UNION ALL
                -- rows without track_id (counted as-is, not de-duped)
                SELECT line, station, status, failed_testcode, process
                FROM {SOURCE_TABLE}
                WHERE {TS_EXPR} >= ?
                  AND (track_id IS NULL OR TRIM(track_id) = '')
            )
        """
        params = [since, since]
    else:
        # No track_id column — use raw rows
        sql = f"""
            WITH deduplicated AS (
                SELECT line, station, status, failed_testcode, process
                FROM {SOURCE_TABLE}
                WHERE {TS_EXPR} >= ?
            )
        """
        params = [since]
    return sql, params


def get_top_lines_above_threshold():
    """
    Return up to TOP_N_LINES lines whose de-duplicated failure rate
    exceeds FAILURE_THRESHOLD and have at least MIN_SAMPLES unique units.
    Each dict: {line, station, total, failed, rate}
    NOTE: This function returns LINE-level stats (grouped by line only),
    and then for each line we later call get_top_testcodes_for_line which
    works at the line level. The alert stores the station for context but
    the rate is line-level.
    """
    try:
        conn  = get_db()
        since = since_str(WINDOW_MINUTES)

        # Quick diagnostic
        chk = conn.execute(f"""
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN UPPER(TRIM(status)) IN ('F','FAIL','FAILED') THEN 1 ELSE 0 END) AS f,
                   MIN({TS_EXPR}) AS earliest,
                   MAX({TS_EXPR}) AS latest
            FROM {SOURCE_TABLE} WHERE {TS_EXPR} >= ?
        """, (since,)).fetchone()
        log.info("Poll window: raw_total=%s  raw_failures=%s  earliest=%s  latest=%s  since=%s",
                 chk['n'], chk['f'], chk['earliest'], chk['latest'], since)

        dedup_cte, params = _dedup_cte(conn, since)

        # Line-level stats (no station grouping so we get full line picture)
        rows = conn.execute(dedup_cte + f"""
            SELECT
                line,
                COUNT(*)  AS total,
                SUM(CASE WHEN UPPER(TRIM(status)) IN ('F','FAIL','FAILED') THEN 1 ELSE 0 END) AS failed
            FROM deduplicated
            GROUP BY line
            HAVING total >= ? AND failed * 100.0 / total > ?
            ORDER BY failed * 100.0 / total DESC
            LIMIT ?
        """, params + [MIN_SAMPLES, FAILURE_THRESHOLD, TOP_N_LINES]).fetchall()

        conn.close()
        result = []
        for r in rows:
            total  = r['total']  or 0
            failed = r['failed'] or 0
            rate   = round(failed / total * 100, 2) if total else 0.0
            result.append({'line': r['line'], 'total': total,
                           'failed': failed, 'rate': rate})

        log.info("Lines above %.1f%% (min=%d, dedup): %s",
                 FAILURE_THRESHOLD, MIN_SAMPLES,
                 [(x['line'], x['rate']) for x in result])
        return result
    except Exception as e:
        log.error("get_top_lines: %s", e, exc_info=True)
        return []


def get_top_testcodes_for_line(line: str):
    """
    For a given line, return the top TOP_N_TESTCODES failed test codes
    (de-duplicated by track_id). Each dict:
      {testcode, process, failed_units, total_units_in_line, rate}
    where failed_units = number of unique units that had this testcode fail.
    """
    try:
        conn  = get_db()
        since = since_str(WINDOW_MINUTES)

        dedup_cte, params = _dedup_cte(conn, since)

        # Split semicolon-joined testcode strings into individual codes,
        # then count how many unique units failed each code.
        # SQLite doesn't have split, so we work with the first code token
        # (up to ';') which is the primary failed test.
        rows = conn.execute(dedup_cte + f"""
            SELECT
                TRIM(SUBSTR(failed_testcode, 1,
                    CASE WHEN INSTR(failed_testcode, ';') > 0
                         THEN INSTR(failed_testcode, ';') - 1
                         ELSE LENGTH(failed_testcode) END)) AS testcode,
                process,
                COUNT(*) AS failed_units
            FROM deduplicated
            WHERE line = ?
              AND UPPER(TRIM(status)) IN ('F','FAIL','FAILED')
              AND failed_testcode IS NOT NULL
              AND TRIM(failed_testcode) != ''
            GROUP BY testcode, process
            ORDER BY failed_units DESC
            LIMIT ?
        """, params + [line, TOP_N_TESTCODES]).fetchall()

        # Also get total units on this line for context
        total_row = conn.execute(dedup_cte + f"""
            SELECT COUNT(*) AS total FROM deduplicated WHERE line = ?
        """, params + [line]).fetchone()
        total_line = total_row['total'] if total_row else 0

        conn.close()
        result = []
        for r in rows:
            fu   = r['failed_units'] or 0
            rate = round(fu / total_line * 100, 1) if total_line else 0.0
            result.append({
                'testcode':     r['testcode'] or 'UNKNOWN',
                'process':      r['process']  or 'N/A',
                'failed_units': fu,
                'total_line':   total_line,
                'rate':         rate,
            })
        return result
    except Exception as e:
        log.error("get_top_testcodes [%s]: %s", line, e, exc_info=True)
        return []


def _get_most_affected_station(line: str, since: str, conn) -> str:
    """Return the station with the highest failure count for this line."""
    try:
        dedup_cte, params = _dedup_cte(conn, since)
        row = conn.execute(dedup_cte + f"""
            SELECT station, COUNT(*) AS failed
            FROM deduplicated
            WHERE line = ?
              AND UPPER(TRIM(status)) IN ('F','FAIL','FAILED')
            GROUP BY station
            ORDER BY failed DESC
            LIMIT 1
        """, params + [line]).fetchone()
        return row['station'] if row else line
    except Exception:
        return line

# ═══════════════════════════════════════════════════════════════════════
# ALERT HELPERS
# ═══════════════════════════════════════════════════════════════════════

def get_open_alert_for_line(line):
    conn = get_db()
    row  = conn.execute(
        "SELECT id FROM failure_alerts WHERE line=? AND resolved=0 "
        "ORDER BY alert_sent_at DESC LIMIT 1", (line,)).fetchone()
    conn.close()
    return row['id'] if row else None


def create_line_alert(line, station, total, failed, rate, top_tcs):
    import json
    tc_json = json.dumps([
        {
            'rank':         i + 1,
            'testcode':     tc['testcode'],
            'process':      tc['process'],
            'failed_units': tc['failed_units'],
            'total_line':   tc['total_line'],
            'rate':         tc['rate'],
        }
        for i, tc in enumerate(top_tcs)
    ])
    sent_at = now_local()
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""
            INSERT INTO failure_alerts
                (source_table, line, station, line_failure_rate,
                 total_tests, failed_tests, top_testcodes, alert_sent_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (SOURCE_TABLE, line, station, rate, total, failed, tc_json, sent_at))
        alert_id = c.lastrowid
        c.execute("""
            INSERT INTO repair_actions
                (alert_id, unit_id, line, station, status, created_at, updated_at)
            VALUES (?,?,?,?,'Open',?,?)
        """, (alert_id, f"ALERT-{alert_id}", line, station, sent_at, sent_at))
        conn.commit()
        log.info("NEW ALERT #%s | %s/%s | %.2f%% | tcs=%d",
                 alert_id, line, station, rate, len(top_tcs))
        return alert_id
    finally:
        conn.close()


def _get_or_create_repair_row(conn, alert_id):
    row = conn.execute(
        "SELECT id FROM repair_actions WHERE alert_id=? ORDER BY id DESC LIMIT 1",
        (alert_id,)).fetchone()
    if row:
        return row['id']
    row = conn.execute(
        "SELECT id FROM repair_actions WHERE unit_id=? ORDER BY id DESC LIMIT 1",
        (f"ALERT-{alert_id}",)).fetchone()
    if row:
        conn.execute("UPDATE repair_actions SET alert_id=? WHERE id=?", (alert_id, row['id']))
        conn.commit()
        return row['id']
    ar = conn.execute(
        "SELECT line, station FROM failure_alerts WHERE id=?", (alert_id,)).fetchone()
    if not ar:
        return None
    now = now_local()
    c = conn.cursor()
    c.execute("""
        INSERT INTO repair_actions
            (alert_id, unit_id, line, station, status, created_at, updated_at)
        VALUES (?,?,?,?,'Open',?,?)
    """, (alert_id, f"ALERT-{alert_id}", ar['line'], ar['station'], now, now))
    conn.commit()
    return c.lastrowid


def save_response(alert_id, field, value, engineer):
    if field not in {'rca_text', 'action_taken', 'capa_text', 'failure_description'}:
        return False
    conn = get_db()
    now  = now_local()
    try:
        alert = conn.execute(
            "SELECT id, line, station FROM failure_alerts WHERE id=?", (alert_id,)).fetchone()
        if not alert:
            log.warning("save_response: alert #%s not found", alert_id)
            return False
        repair_id = _get_or_create_repair_row(conn, alert_id)
        if not repair_id:
            log.warning("save_response: could not find/create repair row for alert #%s", alert_id)
            return False
        conn.execute(
            f"UPDATE repair_actions SET {field}=?, engineer_contact=?, updated_at=? WHERE id=?",
            (value, engineer, now, repair_id))
        ok = conn.total_changes > 0
        conn.commit()
        if ok:
            log.info("SAVED %s | alert #%s | repair #%s | by %s", field, alert_id, repair_id, engineer)
        return ok
    except Exception as e:
        log.error("save_response error: %s", e, exc_info=True)
        return False
    finally:
        conn.close()


def save_media(alert_id, filepath, caption, engineer):
    conn = get_db()
    try:
        alert = conn.execute(
            "SELECT id FROM failure_alerts WHERE id=?", (alert_id,)).fetchone()
        if not alert:
            return False
        repair_id = _get_or_create_repair_row(conn, alert_id)
        if not repair_id:
            return False
        conn.execute("""
            INSERT INTO repair_media (repair_id, alert_id, filepath, caption, uploaded_by, media_type)
            VALUES (?,?,?,?,?,'image')
        """, (repair_id, alert_id, filepath, caption, engineer))
        conn.commit()
        log.info("MEDIA saved | alert #%s | repair #%s | %s", alert_id, repair_id, filepath)
        return True
    except Exception as e:
        log.error("save_media error: %s", e, exc_info=True)
        return False
    finally:
        conn.close()


def resolve_alert(alert_id):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT alert_sent_at, resolved FROM failure_alerts WHERE id=?", (alert_id,)).fetchone()
        if not row:
            return False, False, 0
        if row['resolved']:
            return False, False, 0
        try:
            sent = datetime.strptime(row['alert_sent_at'], '%Y-%m-%d %H:%M:%S')
        except Exception:
            sent = datetime.now()
        mins   = round((datetime.now() - sent).total_seconds() / 60, 1)
        within = 1 if mins <= SLA_HOURS * 60 else 0
        now    = now_local()
        conn.execute(
            "UPDATE failure_alerts SET resolved=1, resolved_at=?, resolution_mins=?, within_sla=? WHERE id=?",
            (now, mins, within, alert_id))
        conn.execute(
            "UPDATE repair_actions SET status='Closed', closed_at=?, updated_at=? WHERE alert_id=?",
            (now, now, alert_id))
        conn.execute(
            "UPDATE repair_actions SET status='Closed', closed_at=?, updated_at=? "
            "WHERE unit_id=? AND (alert_id IS NULL OR alert_id=?)",
            (now, now, f"ALERT-{alert_id}", alert_id))
        conn.commit()
        log.info("RESOLVED alert #%s | %.1f min | within_sla=%s", alert_id, mins, within)
        return True, bool(within), mins
    except Exception as e:
        log.error("resolve_alert error: %s", e, exc_info=True)
        return False, False, 0
    finally:
        conn.close()


def ensure_repair_row(alert_id, line, station):
    conn = get_db()
    try:
        _get_or_create_repair_row(conn, alert_id)
    finally:
        conn.close()

# ═══════════════════════════════════════════════════════════════════════
# ALERT MESSAGE
# ═══════════════════════════════════════════════════════════════════════

def build_alert_text(alert_id, line, station, total, failed, rate, top_tcs):
    tc_block = ""
    for i, tc in enumerate(top_tcs, 1):
        tc_block += (
            f"\n  {i}. TestCode : {tc['testcode']}\n"
            f"     Process  : {tc['process']}\n"
            f"     Failed   : {tc['failed_units']}/{tc['total_line']} ({tc['rate']}%)\n"
        )
    if not tc_block:
        tc_block = "\n  (no testcode data in window)\n"

    return (
        f"⚠️  LINE FAILURE ALERT\n"
        f"{'═' * 40}\n"
        f"Line         : {line}\n"
        f"Station      : {station}\n"
        f"Failure Rate : {rate}%   (threshold >{FAILURE_THRESHOLD}%)\n"
        f"Failed/Total : {failed}/{total}  (last {WINDOW_MINUTES}min, dedup by unit)\n"
        f"Alert ID     : #{alert_id}\n"
        f"Time         : {now_local()}\n"
        f"SLA          : {SLA_HOURS}h\n"
        f"{'─' * 40}\n"
        f"🔴 Top Failed Test Codes:\n{tc_block}"
        f"{'─' * 40}\n"
        f"Reply with:\n"
        f"  RCA:{alert_id}:<root cause>\n"
        f"  ACTION:{alert_id}:<action taken>\n"
        f"  CAPA:{alert_id}:<corrective steps>\n"
        f"  RESOLVE:{alert_id}\n"
        f"Photo: send image, caption=IMG:{alert_id}:<note>\n"
        f"Or tap a button below ↓"
    )


def alert_keyboard(alert_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🧹 Cleaned",    callback_data=f"act_{alert_id}_Cleaned components"),
         InlineKeyboardButton("🔧 Adjusted",   callback_data=f"act_{alert_id}_Adjusted settings")],
        [InlineKeyboardButton("🔩 Replaced",   callback_data=f"act_{alert_id}_Replaced faulty part"),
         InlineKeyboardButton("📐 Calibrated", callback_data=f"act_{alert_id}_Calibrated sensors")],
        [InlineKeyboardButton("🔍 Inspected",  callback_data=f"act_{alert_id}_Inspected and verified"),
         InlineKeyboardButton("♻️ Reset",      callback_data=f"act_{alert_id}_Reset to defaults")],
        [InlineKeyboardButton("✅ MARK RESOLVED", callback_data=f"resolve_{alert_id}")]
    ])

# ═══════════════════════════════════════════════════════════════════════
# AUTO-POLL
# ═══════════════════════════════════════════════════════════════════════

async def poll_and_alert(context: ContextTypes.DEFAULT_TYPE):
    since = since_str(WINDOW_MINUTES)
    log.info("=== AUTO-POLL %s | table=%s | since=%s | threshold=%.1f%% | min=%d ===",
             datetime.now().strftime('%H:%M:%S'), SOURCE_TABLE,
             since, FAILURE_THRESHOLD, MIN_SAMPLES)

    top_lines = get_top_lines_above_threshold()
    if not top_lines:
        log.info("Poll: no lines above %.1f%% with >=%d samples (dedup) in last %dmin",
                 FAILURE_THRESHOLD, MIN_SAMPLES, WINDOW_MINUTES)
        return

    fired = 0
    conn_tmp = get_db()  # for station lookup

    for li in top_lines:
        line = li['line']

        existing = get_open_alert_for_line(line)
        if existing:
            ensure_repair_row(existing, line, line)
            log.info("Poll | %s | alert #%s still OPEN — skip", line, existing)
            continue

        # Identify worst station for display
        station  = _get_most_affected_station(line, since, conn_tmp)
        top_tcs  = get_top_testcodes_for_line(line)
        alert_id = create_line_alert(line, station,
                                     li['total'], li['failed'], li['rate'], top_tcs)
        text     = build_alert_text(alert_id, line, station,
                                    li['total'], li['failed'], li['rate'], top_tcs)
        try:
            msg = await context.bot.send_message(
                chat_id=ALERT_CHAT_ID, text=text, reply_markup=alert_keyboard(alert_id))
            conn = get_db()
            try:
                conn.execute("UPDATE failure_alerts SET telegram_msg_id=? WHERE id=?",
                             (str(msg.message_id), alert_id))
                conn.commit()
            finally:
                conn.close()
            fired += 1
            log.info("✅ ALERT SENT #%s | %s/%s | %.2f%%", alert_id, line, station, li['rate'])
        except Exception as e:
            log.error("send_message FAILED | alert #%s: %s", alert_id, e, exc_info=True)

    conn_tmp.close()
    log.info("Poll complete: %d new alert(s) fired", fired)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled exception", exc_info=context.error)

# ═══════════════════════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"🏭 Factory Failure Monitor\n{'─' * 36}\n"
        f"DB         : {FACTORY_DB}\n"
        f"Table      : {SOURCE_TABLE}\n"
        f"Threshold  : > {FAILURE_THRESHOLD}%\n"
        f"Min samples: {MIN_SAMPLES}\n"
        f"Top lines  : {TOP_N_LINES}  |  Top codes: {TOP_N_TESTCODES}\n"
        f"Window     : {WINDOW_MINUTES} min\n"
        f"Auto-poll  : every {POLL_INTERVAL_SEC}s\n"
        f"SLA        : {SLA_HOURS}h\n{'─' * 36}\n"
        f"/status  /open  /history  /stats  /sla\n"
        f"/check   /debug /reset_alerts  /help"
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📋 Engineer Commands\n─────────────────────────\n"
        "Text replies:\n"
        "  RCA:<id>:<root cause>\n"
        "  ACTION:<id>:<action taken>\n"
        "  CAPA:<id>:<corrective steps>\n"
        "  DESC:<id>:<failure description>\n"
        "  RESOLVE:<id>\n\n"
        "Photo (send image with caption):\n"
        "  IMG:<alert_id>:<note>\n\n"
        "Examples:\n"
        "  RCA:3:Loose RF connector on BE06\n"
        "  ACTION:3:Re-soldered and torque tested\n"
        "  CAPA:3:Daily torque inspection added\n"
        "  DESC:3:MIC harness intermittent\n"
        "  RESOLVE:3\n"
        "  IMG:3:After repair photo"
    )


async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    since = since_str(WINDOW_MINUTES)
    lines = [
        "🔍 DEBUG",
        f"Bot local time : {now_local()}",
        f"Window cutoff  : {since}",
        f"DB path        : {FACTORY_DB}",
        f"DB exists      : {os.path.exists(FACTORY_DB)}",
        f"Table          : {SOURCE_TABLE}  Min={MIN_SAMPLES}",
    ]
    try:
        conn = get_db()
        total_db = conn.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]
        lines.append(f"Total rows in DB: {total_db}")

        chk = conn.execute(f"""
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN UPPER(TRIM(status)) IN ('F','FAIL','FAILED') THEN 1 ELSE 0 END) AS f,
                   MIN({TS_EXPR}) AS earliest, MAX({TS_EXPR}) AS latest
            FROM {SOURCE_TABLE} WHERE {TS_EXPR} >= ?
        """, (since,)).fetchone()
        lines.append(f"In window (raw)   : total={chk['n']}  failures={chk['f']}")
        lines.append(f"  Earliest: {chk['earliest']}")
        lines.append(f"  Latest  : {chk['latest']}")

        has_tid = _has_track_id(conn)
        lines.append(f"track_id column   : {'YES — dedup active' if has_tid else 'NO — using raw rows'}")

        # Dedup counts
        dedup_cte, params = _dedup_cte(conn, since)
        dedup_chk = conn.execute(dedup_cte + """
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN UPPER(TRIM(status)) IN ('F','FAIL','FAILED') THEN 1 ELSE 0 END) AS f
            FROM deduplicated
        """, params).fetchone()
        lines.append(f"In window (dedup) : total={dedup_chk['n']}  failures={dedup_chk['f']}")

        lines.append("\n── Latest 5 rows (raw) ──")
        rows = conn.execute(
            f"SELECT line, station, status, {TS_EXPR} AS norm_ts "
            f"FROM {SOURCE_TABLE} ORDER BY {TS_EXPR} DESC LIMIT 5").fetchall()
        for r in rows:
            in_win = "✅" if r['norm_ts'] and r['norm_ts'] >= since else "❌"
            lines.append(f"  {in_win} {r['line']} | {r['station']} | {r['status']} | {r['norm_ts']}")

        lines.append(f"\n── Dedup line rates (min={MIN_SAMPLES}) ──")
        dedup_cte2, params2 = _dedup_cte(conn, since)
        frows = conn.execute(dedup_cte2 + f"""
            SELECT line,
                   COUNT(*) AS total,
                   SUM(CASE WHEN UPPER(TRIM(status)) IN ('F','FAIL','FAILED') THEN 1 ELSE 0 END) AS failed
            FROM deduplicated
            GROUP BY line HAVING total >= ?
            ORDER BY failed * 100.0 / total DESC LIMIT 10
        """, params2 + [MIN_SAMPLES]).fetchall()
        for fr in frows:
            t  = fr['total']  or 0
            f_ = fr['failed'] or 0
            r  = round(f_ / t * 100, 2) if t else 0
            lines.append(f"  {'⚠️' if r > FAILURE_THRESHOLD else '✅'} "
                         f"{fr['line']}: {f_}/{t} = {r}%")
        if not frows:
            lines.append(f"  No lines with >={MIN_SAMPLES} samples (dedup)")

        oa = conn.execute("SELECT COUNT(*) FROM failure_alerts WHERE resolved=0").fetchone()[0]
        lines.append(f"\nOpen failure_alerts: {oa}")
        conn.close()
    except Exception as e:
        lines.append(f"\nERROR: {e}")
    await update.message.reply_text("\n".join(lines))


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    since = since_str(WINDOW_MINUTES)
    lines = [f"📊 Status (last {WINDOW_MINUTES}min, min {MIN_SAMPLES} samples, dedup by unit)\n"]
    try:
        conn = get_db()
        dedup_cte, params = _dedup_cte(conn, since)
        rows = conn.execute(dedup_cte + f"""
            SELECT line,
                   COUNT(*) AS total,
                   SUM(CASE WHEN UPPER(TRIM(status)) IN ('F','FAIL','FAILED') THEN 1 ELSE 0 END) AS failed
            FROM deduplicated
            GROUP BY line HAVING total >= ?
            ORDER BY failed * 100.0 / total DESC
        """, params + [MIN_SAMPLES]).fetchall()
        for s in rows:
            t  = s['total']  or 0
            f_ = s['failed'] or 0
            r  = round(f_ / t * 100, 2) if t else 0
            lines.append(f"  {'⚠️ ALERT' if r > FAILURE_THRESHOLD else '✅ OK   '} "
                         f"| {s['line']}: {f_}/{t} = {r}%")
        if not rows:
            lines.append(f"  No lines with >={MIN_SAMPLES} samples")
        oa = conn.execute("SELECT COUNT(*) FROM failure_alerts WHERE resolved=0").fetchone()[0]
        ra = conn.execute("SELECT COUNT(*) FROM failure_alerts WHERE resolved=1").fetchone()[0]
        conn.close()
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")
        return
    lines.append(f"\nOpen: {oa}  Resolved: {ra}  Threshold: >{FAILURE_THRESHOLD}%")
    await update.message.reply_text("\n".join(lines))


async def cmd_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import json
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT fa.id, fa.line, fa.station, fa.line_failure_rate,
                   fa.failed_tests, fa.total_tests, fa.top_testcodes, fa.alert_sent_at,
                   ra.id AS ra_id, ra.rca_text, ra.action_taken, ra.capa_text,
                   ra.failure_description, ra.engineer_contact, ra.status AS ra_status
            FROM failure_alerts fa
            LEFT JOIN repair_actions ra ON ra.alert_id = fa.id
            WHERE fa.resolved=0
            GROUP BY fa.id
            ORDER BY fa.alert_sent_at DESC
        """).fetchall()
    except Exception as e:
        conn.close()
        await update.message.reply_text(f"DB error: {e}")
        return
    conn.close()
    if not rows:
        await update.message.reply_text("✅ No open alerts right now.")
        return
    lines = [f"🚨 Open Alerts ({len(rows)})\n"]
    for r in rows:
        try:
            sent  = datetime.strptime(r['alert_sent_at'], '%Y-%m-%d %H:%M:%S')
            age_m = int((datetime.now() - sent).total_seconds() / 60)
        except Exception:
            age_m = 0
        sla = "🔴 SLA BREACH" if age_m > SLA_HOURS * 60 else f"{age_m}m/{SLA_HOURS * 60}m"
        tc_summary = ""
        try:
            for tc in json.loads(r['top_testcodes'] or '[]'):
                fu = tc.get('failed_units', tc.get('failed', '?'))
                tl = tc.get('total_line',  tc.get('total',  '?'))
                tc_summary += (f"\n    {tc['rank']}. {tc['testcode']} "
                               f"{tc['rate']}% ({fu}/{tl})")
        except Exception:
            tc_summary = " (n/a)"
        lines.append(
            f"\n{'─' * 32}\n"
            f"Alert #{r['id']} | {r['line']} | {r['station']}\n"
            f"  Rate   : {r['line_failure_rate']}%  ({r['failed_tests']}/{r['total_tests']} dedup units)\n"
            f"  SLA    : {sla}\n"
            f"  Codes  :{tc_summary}\n"
            f"  Desc   : {r['failure_description'] or '❌ not filled'}\n"
            f"  RCA    : {r['rca_text']             or '❌ not filled'}\n"
            f"  Action : {r['action_taken']         or '❌ not filled'}\n"
            f"  CAPA   : {r['capa_text']            or '❌ not filled'}\n"
            f"  Eng    : {r['engineer_contact']     or '❌ not filled'}"
        )
    await update.message.reply_text("\n".join(lines))


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import json
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT fa.id, fa.line, fa.station, fa.line_failure_rate,
                   fa.failed_tests, fa.total_tests, fa.alert_sent_at,
                   fa.resolved_at, fa.resolution_mins, fa.within_sla,
                   ra.rca_text, ra.action_taken, ra.capa_text,
                   ra.failure_description, ra.engineer_contact
            FROM failure_alerts fa
            LEFT JOIN repair_actions ra ON ra.alert_id = fa.id
            WHERE fa.resolved=1
            GROUP BY fa.id
            ORDER BY fa.resolved_at DESC LIMIT 10
        """).fetchall()
    except Exception as e:
        conn.close()
        await update.message.reply_text(f"DB error: {e}")
        return
    conn.close()
    if not rows:
        await update.message.reply_text("No resolved alerts yet.")
        return
    lines = [f"📋 Last {len(rows)} Resolved Alerts\n"]
    for r in rows:
        sla_icon = "✅" if r['within_sla'] else "🔴"
        lines.append(
            f"\n{'─' * 32}\n"
            f"{sla_icon} Alert #{r['id']} | {r['line']} | {r['station']}\n"
            f"  Rate     : {r['line_failure_rate']}%  ({r['failed_tests']}/{r['total_tests']} dedup units)\n"
            f"  Alerted  : {r['alert_sent_at']}\n"
            f"  Resolved : {r['resolved_at']}  ({r['resolution_mins']}min)\n"
            f"  Desc     : {r['failure_description'] or '—'}\n"
            f"  RCA      : {r['rca_text']             or '—'}\n"
            f"  Action   : {r['action_taken']         or '—'}\n"
            f"  CAPA     : {r['capa_text']            or '—'}\n"
            f"  Engineer : {r['engineer_contact']     or '—'}"
        )
    await update.message.reply_text("\n".join(lines))


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db()
    try:
        tot   = conn.execute("SELECT COUNT(*) FROM failure_alerts").fetchone()[0]
        open_ = conn.execute("SELECT COUNT(*) FROM failure_alerts WHERE resolved=0").fetchone()[0]
        res   = conn.execute("SELECT COUNT(*) FROM failure_alerts WHERE resolved=1").fetchone()[0]
        sla_  = conn.execute("SELECT COUNT(*) FROM failure_alerts WHERE resolved=1 AND within_sla=1").fetchone()[0]
        avg   = conn.execute("SELECT AVG(resolution_mins) FROM failure_alerts WHERE resolved=1").fetchone()[0]
        top_line = conn.execute(
            "SELECT line, COUNT(*) AS c FROM failure_alerts GROUP BY line ORDER BY c DESC LIMIT 1").fetchone()
        conn.close()
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")
        return
    lines = [
        "📈 Alert Statistics",
        f"{'─' * 30}",
        f"Total alerts  : {tot}",
        f"Open          : {open_}",
        f"Resolved      : {res}",
        f"Within SLA    : {sla_}/{res} ({round(sla_ / res * 100, 1) if res else 0}%)",
        f"Avg resolve   : {round(avg, 1) if avg else '—'} min",
        f"Worst line    : {top_line['line'] if top_line else '—'} ({top_line['c'] if top_line else 0} alerts)",
    ]
    await update.message.reply_text("\n".join(lines))


async def cmd_sla(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db()
    r = conn.execute("""
        SELECT COUNT(*) t, SUM(within_sla) w, AVG(resolution_mins) avg,
               MIN(resolution_mins) mn, MAX(resolution_mins) mx
        FROM failure_alerts WHERE resolved=1""").fetchone()
    conn.close()
    t = r['t'] or 0
    if not t:
        await update.message.reply_text("No resolved alerts yet.")
        return
    w = r['w'] or 0
    await update.message.reply_text(
        f"📈 SLA Report\nResolved  : {t}\nWithin SLA: {w}/{t} ({round(w / t * 100, 1)}%)\n"
        f"Avg: {round(r['avg'], 1)}min  Fast: {round(r['mn'], 1)}min  Slow: {round(r['mx'], 1)}min")


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"🔄 Force-polling {SOURCE_TABLE}...\n"
        f"Window={WINDOW_MINUTES}min | Threshold>{FAILURE_THRESHOLD}% | Min={MIN_SAMPLES} (dedup)")
    await poll_and_alert(context)
    await update.message.reply_text("Done. Use /open to see alerts.")


async def cmd_reset_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db()
    try:
        now = now_local()
        cnt = conn.execute("SELECT COUNT(*) FROM failure_alerts WHERE resolved=0").fetchone()[0]
        conn.execute(
            "UPDATE failure_alerts SET resolved=1, resolved_at=?, resolution_mins=0, within_sla=0 WHERE resolved=0",
            (now,))
        conn.execute(
            "UPDATE repair_actions SET status='Closed', closed_at=?, updated_at=? WHERE status='Open'",
            (now, now))
        conn.commit()
    finally:
        conn.close()
    await update.message.reply_text(f"♻️ Closed {cnt} alert(s). Next poll in {POLL_INTERVAL_SEC}s.")

# ═══════════════════════════════════════════════════════════════════════
# TEXT + PHOTO HANDLERS
# ═══════════════════════════════════════════════════════════════════════

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user = update.message.from_user.username or update.message.from_user.first_name or "Engineer"
    up   = text.upper()

    for prefix, field, label in [
        ('RCA:',    'rca_text',            'RCA'),
        ('ACTION:', 'action_taken',        'Action'),
        ('CAPA:',   'capa_text',           'CAPA'),
        ('DESC:',   'failure_description', 'Description'),
    ]:
        if up.startswith(prefix):
            parts = text.split(':', 2)
            if len(parts) < 3 or not parts[1].strip() or not parts[2].strip():
                await update.message.reply_text(f"Format: {prefix}<id>:<text>")
                return
            try:
                alert_id = int(parts[1].strip())
            except ValueError:
                await update.message.reply_text("Alert ID must be a number.")
                return
            ok = save_response(alert_id, field, parts[2].strip(), user)
            await update.message.reply_text(
                f"✅ Saved {label} for Alert #{alert_id}" if ok else
                f"❌ Alert #{alert_id} not found. Use /open or /history.")
            return

    if up.startswith('RESOLVE:'):
        parts = text.split(':', 1)
        if len(parts) < 2 or not parts[1].strip():
            await update.message.reply_text("Format: RESOLVE:<id>")
            return
        try:
            alert_id = int(parts[1].strip())
        except ValueError:
            await update.message.reply_text("Alert ID must be a number.")
            return
        ok, within, mins = resolve_alert(alert_id)
        if not ok:
            await update.message.reply_text(f"❌ Alert #{alert_id} not found or already resolved.")
        elif within:
            await update.message.reply_text(f"✅ Alert #{alert_id} RESOLVED in {mins}min — Within SLA!")
        else:
            await update.message.reply_text(f"⚠️ Alert #{alert_id} resolved in {mins}min — SLA BREACHED")
        return

    await update.message.reply_text("Unknown command. Use /help.")


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caption = (update.message.caption or "").strip()
    user    = update.message.from_user.username or update.message.from_user.first_name or "Engineer"
    if not caption.upper().startswith("IMG:"):
        await update.message.reply_text(
            "📸 To attach photo to an alert:\n  IMG:<alert_id>:<note>\n  Example: IMG:3:After repair")
        return
    parts = caption.split(':', 2)
    if len(parts) < 2 or not parts[1].strip():
        await update.message.reply_text("Format: IMG:<alert_id>:<note>")
        return
    try:
        alert_id = int(parts[1].strip())
    except ValueError:
        await update.message.reply_text("Alert ID must be a number.")
        return
    note  = parts[2].strip() if len(parts) > 2 else ""
    photo = update.message.photo[-1]
    f_obj = await photo.get_file()
    ts    = datetime.now().strftime('%Y%m%d_%H%M%S')
    fname = f"alert{alert_id}_{ts}_{user}.jpg"
    fpath = os.path.join(MEDIA_DIR, fname)
    await f_obj.download_to_drive(fpath)
    ok = save_media(alert_id, fpath, note, user)
    if ok:
        await update.message.reply_text(
            f"📸 Photo saved for Alert #{alert_id}\nNote: {note or '(none)'}\nFile: {fname}")
    else:
        await update.message.reply_text(f"❌ Alert #{alert_id} not found. Use /open.")


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user.username or query.from_user.first_name or "Engineer"

    if data.startswith('act_'):
        _, aid_str, action = data.split('_', 2)
        alert_id = int(aid_str)
        ok = save_response(alert_id, 'action_taken', action, user)
        if ok:
            await query.edit_message_text(
                f"✅ Action saved for Alert #{alert_id}: {action}\n\n"
                f"Fill remaining:\n  RCA:{alert_id}:<root cause>\n"
                f"  CAPA:{alert_id}:<prevention>\n  RESOLVE:{alert_id}\n"
                f"Or send photo: IMG:{alert_id}:<note>",
                reply_markup=alert_keyboard(alert_id))
        else:
            await query.edit_message_text(f"❌ Alert #{alert_id} not found.")
        return

    if data.startswith('resolve_'):
        alert_id = int(data.split('_')[1])
        ok, within, mins = resolve_alert(alert_id)
        if not ok:
            await query.edit_message_text(f"❌ Alert #{alert_id} not found or already resolved.")
        elif within:
            await query.edit_message_text(f"✅ Alert #{alert_id} RESOLVED in {mins}min — Within SLA!")
        else:
            await query.edit_message_text(f"⚠️ Alert #{alert_id} resolved in {mins}min — SLA BREACHED")

# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    acquire_lock()
    import atexit
    atexit.register(release_lock)
    init_db()

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("status",       cmd_status))
    app.add_handler(CommandHandler("open",         cmd_open))
    app.add_handler(CommandHandler("history",      cmd_history))
    app.add_handler(CommandHandler("stats",        cmd_stats))
    app.add_handler(CommandHandler("sla",          cmd_sla))
    app.add_handler(CommandHandler("check",        cmd_check))
    app.add_handler(CommandHandler("reset_alerts", cmd_reset_alerts))
    app.add_handler(CommandHandler("debug",        cmd_debug))
    app.add_handler(MessageHandler(filters.PHOTO,                   handle_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_error_handler(error_handler)

    app.job_queue.run_repeating(poll_and_alert, interval=POLL_INTERVAL_SEC, first=10)

    log.info("=" * 60)
    log.info("Factory Monitor | DB=%s | Table=%s", FACTORY_DB, SOURCE_TABLE)
    log.info("Threshold>%.1f%% | Min=%d | TopLines=%d | TopCodes=%d | Poll=%ss | SLA=%sh",
             FAILURE_THRESHOLD, MIN_SAMPLES, TOP_N_LINES, TOP_N_TESTCODES,
             POLL_INTERVAL_SEC, SLA_HOURS)
    log.info("Media dir: %s", MEDIA_DIR)
    log.info("Dedup mode: by track_id (latest status per unit per window)")
    log.info("First auto-poll in 10s")
    log.info("=" * 60)

    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()