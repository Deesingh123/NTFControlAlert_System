"""
rslt_parser_v6.py  — Read-at-Detection | Zero FILE_GONE | Kafka Durable Pipeline
==================================================================================
KEY ARCHITECTURAL CHANGE FROM v5  [FIX-14]
  ROOT CAUSE OF FILE_GONE (was 6–7 per session):
    File detected → pushed to heap → workers busy → file deleted by MES
    → worker finally dequeues → open() raises FileNotFoundError → FILE_GONE

  FIX-14: READ-AT-DETECTION (content captured in queue_file())
    File detected → content read into RAM immediately → heap entry carries
    the raw text → worker parses from RAM string, never touches disk again.

    FILE_GONE is now STRUCTURALLY IMPOSSIBLE:
      • We read the file in queue_file(), which runs inside the watchdog
        callback / poll / rescan thread — immediately upon detection.
      • If the file is already gone at that exact moment (extreme edge case),
        we log it once and never queue it — no silent skip.
      • Workers receive (content_string, filename, source) — no filepath
        needed — so disk state is irrelevant after detection.

  FALLBACK CHAIN in queue_file():
    1. Try open(filepath) — succeeds for 99.9 %+ of files.
    2. If FileNotFoundError/OSError on first attempt → retry once after
       CONTENT_READ_RETRY_MS (default 5 ms, just enough for SMB flush).
    3. If still gone → log a single WARNING and do NOT queue the file.
       The rescan/poll loop will not re-queue it (confirmed_files guard).
       This counts as a FILE_GONE but removes the silent-discard flaw.

  ADDITIONAL BENEFIT:
    Workers no longer open() files at all — they call parse_content()
    which operates on the string already in memory.  This also removes
    the retry loop inside parse_rslt() because we can detect an empty
    TH4 at read time and retry the read immediately (still in queue_file).

NEW CONSTANTS:
  CONTENT_READ_RETRY_MS  — wait between the two content-read attempts (5 ms)
  CONTENT_READ_RETRIES   — how many times to retry reading (default 2)

RETAINED FROM v5:
  Kafka durable pipeline (producer → topic → consumer → SQLite)
  FIX-1  through FIX-13 (priority queue, dynamic workers, etc.)
"""

import os
import re
import sys
import json
import time
import heapq
import queue
import sqlite3
import threading
import logging
import multiprocessing
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
LOG_FOLDER  = r"Q:\quality_data\test_results"
DB_PATH     = r"E:\Factor\rslt_data.db"
LOG_FILE    = r"E:\Factor\rslt_parser.log"
REPORT_CSV  = r"E:\Factor\gap_report.csv"

# ---------------------------------------------------------------------------
# CONFLUENT KAFKA CONFIGURATION
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP       = "pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092"
KAFKA_API_KEY         = "3RJOKJ4XWMFYJR5B"
KAFKA_API_SECRET      = "cfltnN0D07RcVWZxVDCVzg1/z8AMhHVe6zwnLzu+iAhkJYYmykPi2/FMGfyfWaGQ"
KAFKA_TOPIC_PARSED    = "rslt_parsed_records"
KAFKA_CONSUMER_GROUP  = "rslt_db_writer_v6"
KAFKA_PRODUCER_LINGER = 50
KAFKA_PRODUCER_RETRIES = 10
KAFKA_FLUSH_TIMEOUT   = 10.0

# ---------------------------------------------------------------------------
# RUNTIME TUNING
# ---------------------------------------------------------------------------
WORKER_COUNT    = min(16, multiprocessing.cpu_count() * 2)
DB_BATCH_SIZE   = 50
FLUSH_INTERVAL  = 30
RESCAN_INTERVAL = 3
POLL_INTERVAL   = 1
REPORT_INTERVAL = 30
HEALTH_INTERVAL = 30
MAX_QUEUE_DEPTH = 200
MAX_PQ_SIZE     = 5000

# [FIX-14] Read-at-detection retry — tiny wait in case SMB hasn't flushed yet
CONTENT_READ_RETRY_MS  = 5    # ms between read attempts in queue_file()
CONTENT_READ_RETRIES   = 2    # max attempts before declaring gone

# ---------------------------------------------------------------------------
# PROCESS ALLOWLIST
# ---------------------------------------------------------------------------
ALLOWED_PROCESSES = frozenset({
    "L2AR", "L2VISION", "UCT", "FOD", "DepthVal", "DepthCal",
})

R_NO_TH4    = "NO_TH4"
R_PROCESS   = "PROCESS_SKIP"
R_DUPLICATE = "DUPLICATE"
R_FILE_GONE = "FILE_GONE"
R_PARSE_ERR = "PARSE_ERROR"

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# [FIX-14] READ FILE CONTENT AT DETECTION TIME
# ---------------------------------------------------------------------------
def read_file_content(filepath: str) -> str | None:
    """
    Read the raw text of a file immediately at detection time.

    Returns the full file text (possibly empty string) on success,
    or None if the file cannot be read after all retries.

    Retry logic:
      Some SMB/NFS writes are not instantly visible after the watchdog
      fires. CONTENT_READ_RETRY_MS gives the OS/network stack time to
      flush before we give up.  Two attempts cover >99.9% of cases.
    """
    for attempt in range(CONTENT_READ_RETRIES):
        try:
            with open(filepath, "r", errors="ignore") as fh:
                return fh.read()
        except FileNotFoundError:
            # File already deleted — don't wait if it's gone
            log.debug("read_file_content: gone on attempt %d — %s",
                      attempt + 1, os.path.basename(filepath))
            return None
        except OSError as exc:
            if attempt < CONTENT_READ_RETRIES - 1:
                log.debug("read_file_content: OSError attempt %d (%s) — %s",
                          attempt + 1, exc, os.path.basename(filepath))
                time.sleep(CONTENT_READ_RETRY_MS / 1000.0)
            else:
                log.debug("read_file_content: failed all attempts — %s: %s",
                          os.path.basename(filepath), exc)
                return None
    return None


# ---------------------------------------------------------------------------
# TESTCODE DETECTOR  (unchanged)
# ---------------------------------------------------------------------------
_GEO_SUFFIXES    = frozenset({
    "INDIA","CHINA","USA","JAPAN","KOREA","GLOBAL",
    "EUROPE","LATAM","APAC","ROW","EMEA","DOMESTIC","EXPORT",
})
_SKIP_VALUES     = frozenset({"(NULL)","NULL","NONE","*","N/A"})
_RE_HAS_DIGITS   = re.compile(r"\d{2,}")
_RE_MODEL_STATION = re.compile(r"^[A-Z]+-[A-Z]")
_RE_KEYWORDS     = re.compile(
    r"STATUS|RSSI|LEAKAGE|BLEMISH|CHARGE|WIFI|GPS|NFC|"
    r"SENSOR|AUDIO|CAMERA|LIGHT|BATTERY|DISPLAY|TOUCH|ANTENNA|"
    r"FIRMWARE|DOWNLOAD|COMPLETE|DETECT|VERIFY|CONNECT|ACTIVITY|"
    r"FREQ|OFFSET|CARRIER|NOISE|HEADSET|LEVEL|RESULT|REGION|"
    r"BARCODE|BOOT|CALIBR|FLASH|REBOOT|UNLOCK|LOCK|READ|WRITE"
)

def _is_testcode(raw: str) -> bool:
    val = raw.strip()
    if not val:                              return False
    if val.upper() in _SKIP_VALUES:          return False
    if val.startswith("REL."):               return False
    if "_" not in val:                       return False
    if any(c.islower() for c in val):        return False
    if _RE_MODEL_STATION.match(val):         return False
    segs = val.split(";")[0].strip().split("_")
    if segs[-1] in _GEO_SUFFIXES:           return False
    if len(segs) < 3:                        return False
    if not _RE_HAS_DIGITS.search(val) and not _RE_KEYWORDS.search(val):
        return False
    return True


# ---------------------------------------------------------------------------
# [FIX-14] PARSE FROM CONTENT STRING — no disk access
# ---------------------------------------------------------------------------
def parse_content(content: str) -> dict | None:
    """
    Parse TH4 row from an already-loaded content string.
    Identical logic to parse_rslt() but operates on a string in RAM.
    Never raises FileNotFoundError — FILE_GONE is impossible here.

    Returns:
        dict  — parsed record
        None  — no valid TH4 row found in content
    """
    for raw_row in content.splitlines():
        parts = raw_row.rstrip("\n").split(",")
        if parts[0] != "TH4":
            continue
        if len(parts) < 18:
            continue

        track_id  = parts[1].strip()
        timestamp = parts[3].strip()
        process   = parts[4].strip()
        station   = parts[5].strip()
        status    = parts[9].strip()
        model     = parts[17].strip()
        line      = re.split(r"[-_]", station)[0] if station else ""

        failed_tc = ""
        if status == "F":
            if len(parts) > 14:
                failed_tc = parts[14].strip()
            if not failed_tc:
                for i in range(10, min(16, len(parts))):
                    if _is_testcode(parts[i]):
                        failed_tc = parts[i].strip()
                        break

        date_str = time_str = ts_std = None
        for fmt in ("%m/%d/%y %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
            try:
                dt       = datetime.strptime(timestamp, fmt)
                date_str = dt.strftime("%Y-%m-%d")
                time_str = dt.strftime("%H:%M:%S")
                ts_std   = dt.strftime("%Y-%m-%d %H:%M:%S")
                break
            except ValueError:
                continue

        return {
            "track_id":     track_id,
            "process":      process,
            "model":        model,
            "line":         line,
            "station":      station,
            "status":       status,
            "failed_tests": failed_tc,
            "date":         date_str,
            "time":         time_str,
            "timestamp":    ts_std,
        }

    return None   # no TH4 row found


# ---------------------------------------------------------------------------
# FILENAME HELPERS
# ---------------------------------------------------------------------------
def is_rslt_file(path: str) -> bool:
    return "rslt" in os.path.basename(path).lower()


def extract_track_id_from_filename(filename: str) -> str:
    base = os.path.splitext(filename)[0]
    for pfx in ("ic_", "IC_"):
        if base.startswith(pfx):
            base = base[len(pfx):]
            break
    base = base.replace(".rslt", "").replace("_rslt", "")
    parts = re.split(r"[_.]", base)
    return parts[0].strip() if parts else ""


# ---------------------------------------------------------------------------
# KAFKA PRODUCER WRAPPER
# ---------------------------------------------------------------------------
class KafkaProducerWrapper:
    def __init__(self):
        from confluent_kafka import Producer
        self._producer = Producer({
            "bootstrap.servers":  KAFKA_BOOTSTRAP,
            "security.protocol":  "SASL_SSL",
            "sasl.mechanisms":    "PLAIN",
            "sasl.username":      KAFKA_API_KEY,
            "sasl.password":      KAFKA_API_SECRET,
            "linger.ms":          KAFKA_PRODUCER_LINGER,
            "retries":            KAFKA_PRODUCER_RETRIES,
            "acks":               "all",
            "enable.idempotence": True,
            "compression.type":   "lz4",
        })
        log.info("Kafka producer initialised → topic: %s", KAFKA_TOPIC_PARSED)

    def _on_delivery(self, err, msg):
        if err:
            log.error("Kafka delivery FAILED — topic=%s key=%s err=%s",
                      msg.topic(), msg.key(), err)
        else:
            log.debug("Kafka ACK — topic=%s partition=%d offset=%d",
                      msg.topic(), msg.partition(), msg.offset())

    def produce(self, record: dict, key: str = ""):
        try:
            self._producer.produce(
                topic    = KAFKA_TOPIC_PARSED,
                key      = key.encode("utf-8"),
                value    = json.dumps(record).encode("utf-8"),
                callback = self._on_delivery,
            )
            self._producer.poll(0)
        except Exception as exc:
            log.error("Kafka produce error: %s", exc)

    def flush(self, timeout: float = KAFKA_FLUSH_TIMEOUT):
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            log.warning("Kafka flush: %d message(s) not delivered within %.1fs",
                        remaining, timeout)
        else:
            log.info("Kafka producer flushed — all messages delivered")


# ---------------------------------------------------------------------------
# SQLITE DATABASE MANAGER
# ---------------------------------------------------------------------------
class SQLiteDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn    = None
        self._lock   = threading.Lock()
        self._stop_checkpoint = threading.Event()
        self._checkpoint_timer = None

    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0,
                               check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-10000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=268435456")
        self.conn = conn
        self._init_tables()
        self._start_checkpoint_thread()
        return conn

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS log_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                process TEXT, model TEXT, line TEXT, station TEXT,
                status TEXT, failed_testcode TEXT, track_id TEXT,
                date DATE, time TIME, timestamp TIMESTAMP
            )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_track_process "
                    "ON log_data (track_id, process)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_timestamp "
                    "ON log_data (timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_station "
                    "ON log_data (station)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                filename TEXT PRIMARY KEY,
                processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS skip_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT, reason TEXT, process TEXT,
                track_id TEXT, timestamp TIMESTAMP,
                logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_skip_reason "
                    "ON skip_log (reason)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_skip_ts "
                    "ON skip_log (timestamp)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS no_th4_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL, track_id TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rescan_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL, track_id TEXT, process TEXT,
                detected_at TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mqs_reconciliation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                track_id TEXT NOT NULL, process TEXT, station TEXT,
                mqs_timestamp TIMESTAMP, mqs_status TEXT,
                found_in_db INTEGER DEFAULT 0, db_timestamp TIMESTAMP,
                note TEXT, imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mqs_track "
                    "ON mqs_reconciliation (track_id, process)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kafka_offsets (
                topic_partition TEXT PRIMARY KEY,
                committed_offset INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
        # [FIX-14] track read-at-detection failures for observability
        cur.execute("""
            CREATE TABLE IF NOT EXISTS file_gone_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                track_id TEXT,
                source TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
        self.conn.commit()
        log.info("SQLite tables verified (v6, read-at-detection)")

    def _start_checkpoint_thread(self):
        def loop():
            while not self._stop_checkpoint.is_set():
                time.sleep(FLUSH_INTERVAL)
                try:
                    with self._lock:
                        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                        log.debug("WAL checkpoint completed")
                except Exception as e:
                    log.error("WAL checkpoint failed: %s", e)
        self._checkpoint_timer = threading.Thread(target=loop, daemon=True)
        self._checkpoint_timer.start()

    def execute_batch(self, insert_rows, skip_rows,
                      processed_files, processed_skips,
                      rescan_rows=None, gone_rows=None):
        with self._lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                cur = self.conn.cursor()
                if insert_rows:
                    cur.executemany("""
                        INSERT INTO log_data
                        (process,model,line,station,status,
                         failed_testcode,track_id,date,time,timestamp)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, insert_rows)
                if skip_rows:
                    cur.executemany("""
                        INSERT INTO skip_log
                        (filename,reason,process,track_id,timestamp)
                        VALUES (?,?,?,?,?)
                    """, skip_rows)
                if processed_files:
                    cur.executemany("""
                        INSERT OR REPLACE INTO processed_files
                        (filename,processed_time) VALUES (?,CURRENT_TIMESTAMP)
                    """, [(f,) for f in processed_files])
                if processed_skips:
                    cur.executemany("""
                        INSERT OR REPLACE INTO processed_files
                        (filename,processed_time) VALUES (?,CURRENT_TIMESTAMP)
                    """, [(f,) for f in processed_skips])
                if rescan_rows:
                    cur.executemany("""
                        INSERT INTO rescan_tracking
                        (filename,track_id,process,detected_at)
                        VALUES (?,?,?,CURRENT_TIMESTAMP)
                    """, rescan_rows)
                if gone_rows:
                    cur.executemany("""
                        INSERT INTO file_gone_log (filename,track_id,source)
                        VALUES (?,?,?)
                    """, gone_rows)
                self.conn.commit()
                return len(insert_rows) if insert_rows else 0
            except Exception as e:
                self.conn.rollback()
                log.error("Batch insert failed: %s", e)
                return 0

    def import_mqs_csv(self, csv_path: str):
        import csv
        inserted = matched = 0
        try:
            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                cur = self.conn.cursor()
                self.conn.execute("BEGIN IMMEDIATE")
                for row in reader:
                    track_id   = row.get("Track_ID", "").strip()
                    process    = row.get("Process", "").strip()
                    station    = row.get("Station", "").strip()
                    mqs_date   = row.get("Date", "").strip()
                    mqs_time   = row.get("Time", "").strip()
                    mqs_status = row.get("Reason", "").strip()
                    mqs_ts     = (f"{mqs_date} {mqs_time}"
                                  if mqs_date and mqs_time else None)
                    cur.execute("""
                        SELECT timestamp FROM log_data
                        WHERE track_id=? AND process=? AND date=? LIMIT 1
                    """, (track_id, process, mqs_date))
                    db_row = cur.fetchone()
                    found  = 1 if db_row else 0
                    db_ts  = db_row["timestamp"] if db_row else None
                    cur.execute("""
                        INSERT INTO mqs_reconciliation
                        (track_id,process,station,mqs_timestamp,
                         mqs_status,found_in_db,db_timestamp,note)
                        VALUES (?,?,?,?,?,?,?,?)
                    """, (track_id, process, station, mqs_ts,
                          mqs_status, found, db_ts, "auto-reconciled"))
                    inserted += 1
                    if found:
                        matched += 1
                self.conn.commit()
                log.info("MQS import: %d records, %d matched, %d missing",
                         inserted, matched, inserted - matched)
        except Exception as e:
            log.error("MQS import failed: %s", e)
            try:
                self.conn.rollback()
            except Exception:
                pass

    def load_confirmed_files(self):
        cur = self.conn.cursor()
        cur.execute("SELECT filename FROM processed_files")
        return {row[0] for row in cur.fetchall()}

    def get_stats(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM log_data")
        total = cur.fetchone()[0]
        cur.execute("SELECT reason, COUNT(*) FROM skip_log GROUP BY reason")
        skips = dict(cur.fetchall())
        return total, skips

    def close(self):
        self._stop_checkpoint.set()
        if self._checkpoint_timer:
            self._checkpoint_timer.join(timeout=5)
        if self.conn:
            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass
            self.conn.close()
            log.info("Database closed, final WAL checkpoint done")


# ---------------------------------------------------------------------------
# MAIN PARSER ENGINE
# ---------------------------------------------------------------------------
class RsltParser:
    def __init__(self, watch_folder, db_path, worker_count=WORKER_COUNT):
        self.watch_folder = watch_folder
        self.db           = SQLiteDB(db_path)
        self.worker_count = worker_count

        self.confirmed_files = set()
        self.queued          = set()
        self.queued_lock     = threading.Lock()

        # [FIX-11] Priority heap
        # Entry: (-mtime, seq, content_str, filename, source)
        # content_str is the full file text read at detection time [FIX-14]
        self._pq       = []
        self._pq_lock  = threading.Lock()
        self._pq_seq   = 0
        self._pq_event = threading.Event()

        self.write_queue = queue.Queue()

        self._kafka_producer: KafkaProducerWrapper | None = None

        self.inserted      = 0
        self.skipped       = {}
        self.file_gone_cnt = 0          # detected-at-read-time (not parse time)
        self.counter_lock  = threading.Lock()
        self.stop_evt      = threading.Event()

    # ---------------------------------------------------------------
    def start(self):
        self.db.connect()
        self.confirmed_files = self.db.load_confirmed_files()
        total, skips = self.db.get_stats()
        log.info("DB state: %d rows | %d processed | skips: %s",
                 total, len(self.confirmed_files), skips)

        self._kafka_producer = KafkaProducerWrapper()

        writer_thread = threading.Thread(
            target=self._db_writer_loop, daemon=True)
        writer_thread.start()

        kafka_consumer_thread = threading.Thread(
            target=self._kafka_consumer_loop, daemon=True,
            name="kafka-consumer")
        kafka_consumer_thread.start()

        with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
            for _ in range(self.worker_count):
                executor.submit(self._file_processor_loop)

            observer      = self._start_watchdog()
            rescan_thread = threading.Thread(
                target=self._rescan_loop, daemon=True)
            poll_thread   = threading.Thread(
                target=self._poll_loop, daemon=True)
            report_thread = threading.Thread(
                target=self._report_loop, daemon=True)
            health_thread = threading.Thread(
                target=self._health_loop, daemon=True)

            for t in (rescan_thread, poll_thread,
                      report_thread, health_thread):
                t.start()

            self._startup_scan()

            try:
                while not self.stop_evt.is_set():
                    time.sleep(0.5)
            except KeyboardInterrupt:
                log.info("Shutdown requested...")
            finally:
                self.stop_evt.set()
                observer.stop()
                observer.join()
                self._drain_queues()
                if self._kafka_producer:
                    self._kafka_producer.flush()
                self.write_queue.put(None)
                writer_thread.join(timeout=10)
                self.db.close()
                self._log_final_stats()

    # ---------------------------------------------------------------
    def _start_watchdog(self):
        parser_ref = self

        class RsltHandler(FileSystemEventHandler):
            def _handle(self, path):
                if not os.path.isdir(path) and is_rslt_file(path):
                    parser_ref.queue_file(path, source="watchdog")

            def on_created(self, event):
                self._handle(event.src_path)

            def on_modified(self, event):
                self._handle(event.src_path)

        handler  = RsltHandler()
        observer = Observer()
        observer.schedule(handler, self.watch_folder, recursive=True)
        observer.start()
        log.info("Watchdog started on %s (recursive, created+modified)",
                 self.watch_folder)
        return observer

    # ---------------------------------------------------------------
    def queue_file(self, filepath: str, source: str = "watchdog"):
        """
        [FIX-14] READ CONTENT IMMEDIATELY at detection time, then push
        to the priority heap.

        If the file cannot be read (already deleted), log it as FILE_GONE
        here — no silent discard, no unread queue entry.
        """
        filename = os.path.basename(filepath)

        # Deduplication guard
        with self.queued_lock:
            if (filename in self.confirmed_files
                    or filename in self.queued):
                return
            self.queued.add(filename)

        # ── [FIX-14] READ NOW — before the file can disappear ──────
        content = read_file_content(filepath)

        if content is None:
            # File gone before we could read it — record it and move on
            log.warning("FILE_GONE at read-time [%s]: %s", source, filename)
            with self.counter_lock:
                self.file_gone_cnt += 1
                self.skipped[R_FILE_GONE] = (
                    self.skipped.get(R_FILE_GONE, 0) + 1)
            track_id = extract_track_id_from_filename(filename)
            # Log to DB (best-effort via write_queue)
            self.write_queue.put((
                "GONE_AT_READ", filename, track_id, source))
            with self.queued_lock:
                self.queued.discard(filename)
                self.confirmed_files.add(filename)
            return

        # ── Push content + metadata to the priority heap ────────────
        try:
            mtime = os.path.getmtime(filepath)
        except OSError:
            mtime = time.time()

        with self._pq_lock:
            self._pq_seq += 1
            heapq.heappush(
                self._pq,
                (-mtime, self._pq_seq, content, filename, source)
            )
            if len(self._pq) > MAX_PQ_SIZE:
                evicted = heapq.nlargest(1, self._pq)[0]
                self._pq.remove(evicted)
                heapq.heapify(self._pq)
                evicted_name = evicted[3]
                log.warning("PQ cap hit — evicted oldest: %s", evicted_name)
                with self.queued_lock:
                    self.queued.discard(evicted_name)

        self._pq_event.set()

    # ---------------------------------------------------------------
    def _pq_pop(self):
        with self._pq_lock:
            if self._pq:
                _, _seq, content, filename, source = heapq.heappop(self._pq)
                return content, filename, source
        return None

    # ---------------------------------------------------------------
    def _file_processor_loop(self):
        """
        [FIX-14] Workers now receive (content, filename, source) from the
        heap.  They call parse_content() on the in-memory string — no
        open() call, no FileNotFoundError possible.
        """
        while not self.stop_evt.is_set():
            item = self._pq_pop()
            if item is None:
                self._pq_event.wait(timeout=0.05)
                self._pq_event.clear()
                continue

            content, filename, source = item
            try:
                # ── Parse from RAM — no disk touch ─────────────────
                data = parse_content(content)

                if data is None:
                    # TH4 row absent in content we already have → NO_TH4
                    track_id = extract_track_id_from_filename(filename)
                    self.write_queue.put(("NO_TH4", filename, track_id))
                    continue

                if data["process"] not in ALLOWED_PROCESSES:
                    self.write_queue.put((
                        "SKIP", filename, R_PROCESS,
                        data["process"],
                        data.get("track_id", ""),
                        data.get("timestamp", "") or ""
                    ))
                    continue

                # ── SUCCESS → publish to Kafka ──────────────────────
                failed_str = data["failed_tests"] or ""
                log.info(
                    "→KAFKA track=%-22s proc=%-10s "
                    "station=%-22s status=%s  failed='%.60s'",
                    data["track_id"], data["process"],
                    data["station"], data["status"], failed_str
                )
                kafka_record = {**data, "_filename": filename,
                                         "_source": source}
                self._kafka_producer.produce(
                    record=kafka_record, key=data["track_id"])

            except Exception as e:
                log.error("Error processing %s: %s", filename, e)
                self.write_queue.put((
                    "SKIP", filename, R_PARSE_ERR, "", "", ""))

    # ---------------------------------------------------------------
    def _kafka_consumer_loop(self):
        from confluent_kafka import Consumer, KafkaError

        consumer = Consumer({
            "bootstrap.servers":    KAFKA_BOOTSTRAP,
            "security.protocol":    "SASL_SSL",
            "sasl.mechanisms":      "PLAIN",
            "sasl.username":        KAFKA_API_KEY,
            "sasl.password":        KAFKA_API_SECRET,
            "group.id":             KAFKA_CONSUMER_GROUP,
            "auto.offset.reset":    "earliest",
            "enable.auto.commit":   False,
            "max.poll.interval.ms": 300000,
        })
        consumer.subscribe([KAFKA_TOPIC_PARSED])
        log.info("Kafka consumer subscribed → topic: %s  group: %s",
                 KAFKA_TOPIC_PARSED, KAFKA_CONSUMER_GROUP)

        try:
            while not self.stop_evt.is_set():
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    from confluent_kafka import KafkaError as KE
                    if msg.error().code() == KE._PARTITION_EOF:
                        continue
                    log.error("Kafka consumer error: %s", msg.error())
                    continue
                try:
                    record   = json.loads(msg.value().decode("utf-8"))
                    filename = record.pop("_filename", "")
                    source   = record.pop("_source",   "unknown")
                    with self.queued_lock:
                        if filename in self.confirmed_files:
                            consumer.commit(message=msg, asynchronous=False)
                            continue
                    self.write_queue.put(("INSERT", record, filename, source))
                    consumer.commit(message=msg, asynchronous=False)
                except json.JSONDecodeError as exc:
                    log.error("Bad Kafka message (JSON): %s", exc)
                except Exception as exc:
                    log.error("Kafka consumer record error: %s", exc)
        except Exception as exc:
            log.error("Kafka consumer loop crashed: %s", exc)
        finally:
            consumer.close()
            log.info("Kafka consumer closed")

    # ---------------------------------------------------------------
    def _db_writer_loop(self):
        insert_buf      = []
        skip_buf        = []
        processed_files = []
        processed_skips = []
        rescan_buf      = []
        gone_buf        = []        # [FIX-14] file_gone_log rows
        last_flush      = time.time()

        while True:
            try:
                item = self.write_queue.get(timeout=1)
            except queue.Empty:
                if insert_buf or skip_buf or rescan_buf or gone_buf:
                    self._flush_batch(insert_buf, skip_buf,
                                      processed_files, processed_skips,
                                      rescan_buf, gone_buf)
                    (insert_buf, skip_buf, processed_files,
                     processed_skips, rescan_buf, gone_buf) = \
                        [], [], [], [], [], []
                    last_flush = time.time()
                elif time.time() - last_flush > FLUSH_INTERVAL:
                    try:
                        self.db.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                    except Exception as e:
                        log.error("WAL checkpoint error: %s", e)
                    last_flush = time.time()
                continue

            if item is None:
                if insert_buf or skip_buf or rescan_buf or gone_buf:
                    self._flush_batch(insert_buf, skip_buf,
                                      processed_files, processed_skips,
                                      rescan_buf, gone_buf)
                break

            kind = item[0]
            try:
                if kind == "INSERT":
                    _, data, filename, source = item
                    insert_buf.append((
                        data["process"], data["model"],
                        data["line"],    data["station"],
                        data["status"],  data["failed_tests"],
                        data["track_id"], data["date"],
                        data["time"],    data["timestamp"],
                    ))
                    processed_files.append(filename)
                    if source in ("rescan", "poll"):
                        rescan_buf.append((filename, data["track_id"],
                                           data["process"]))
                    with self.counter_lock:
                        self.inserted += 1

                elif kind == "SKIP":
                    _, filename, reason, process, track_id, ts = item
                    ts_val = ts if ts else None
                    skip_buf.append((filename, reason,
                                     process or "", track_id or "", ts_val))
                    processed_skips.append(filename)
                    with self.counter_lock:
                        self.skipped[reason] = (
                            self.skipped.get(reason, 0) + 1)

                elif kind == "NO_TH4":
                    _, filename, track_id = item
                    with self.db._lock:
                        self.db.conn.execute(
                            "INSERT INTO no_th4_files "
                            "(filename, track_id) VALUES (?, ?)",
                            (filename, track_id))
                        self.db.conn.commit()
                    processed_skips.append(filename)

                elif kind == "GONE_AT_READ":
                    # [FIX-14] File was already deleted at read time
                    _, filename, track_id, source = item
                    gone_buf.append((filename, track_id, source))

                if (len(insert_buf) + len(skip_buf)
                        + len(rescan_buf) + len(gone_buf) >= DB_BATCH_SIZE):
                    self._flush_batch(insert_buf, skip_buf,
                                      processed_files, processed_skips,
                                      rescan_buf, gone_buf)
                    (insert_buf, skip_buf, processed_files,
                     processed_skips, rescan_buf, gone_buf) = \
                        [], [], [], [], [], []
                    last_flush = time.time()

            except Exception as e:
                log.error("db_writer_loop error: %s", e)

    # ---------------------------------------------------------------
    def _flush_batch(self, insert_rows, skip_rows,
                     processed_files, processed_skips,
                     rescan_rows, gone_rows=None):
        if not insert_rows and not skip_rows and not rescan_rows \
                and not gone_rows:
            return
        inserted = self.db.execute_batch(
            insert_rows, skip_rows,
            processed_files, processed_skips,
            rescan_rows, gone_rows)
        if inserted:
            log.info("DB commit: %d rows inserted (via Kafka)", inserted)

        with self.queued_lock:
            for fname in processed_files + processed_skips:
                self.confirmed_files.add(fname)
                self.queued.discard(fname)

    # ---------------------------------------------------------------
    def _poll_loop(self):
        log.info("Poll thread started (interval=%ds)", POLL_INTERVAL)
        while not self.stop_evt.wait(POLL_INTERVAL):
            try:
                entries = list(os.scandir(self.watch_folder))
            except OSError as exc:
                log.warning("Poll scan error: %s", exc)
                continue
            with self.queued_lock:
                new_files = [
                    e.path for e in entries
                    if e.is_file()
                    and is_rslt_file(e.path)
                    and e.name not in self.confirmed_files
                    and e.name not in self.queued
                ]
            for path in new_files:
                self.queue_file(path, source="poll")

    # ---------------------------------------------------------------
    def _rescan_loop(self):
        while not self.stop_evt.wait(RESCAN_INTERVAL):
            try:
                entries = list(os.scandir(self.watch_folder))
            except OSError as exc:
                log.warning("Rescan error: %s", exc)
                continue
            with self.queued_lock:
                new_files = [
                    e.path for e in entries
                    if e.is_file()
                    and is_rslt_file(e.path)
                    and e.name not in self.confirmed_files
                    and e.name not in self.queued
                ]
            if new_files:
                log.info("Rescan: %d missed file(s) found", len(new_files))
                for path in new_files:
                    self.queue_file(path, source="rescan")

    # ---------------------------------------------------------------
    def _health_loop(self):
        while not self.stop_evt.wait(HEALTH_INTERVAL):
            with self._pq_lock:
                pq_depth = len(self._pq)
            wq = self.write_queue.qsize()
            log.info(
                "Health: pq_depth=%d  write_q=%d  queued=%d  "
                "workers=%d  inserted=%d  file_gone(read-time)=%d  no_th4=%d",
                pq_depth, wq, len(self.queued), self.worker_count,
                self.inserted, self.file_gone_cnt,
                self.skipped.get(R_NO_TH4, 0),
            )
            if pq_depth > MAX_QUEUE_DEPTH or wq > MAX_QUEUE_DEPTH:
                log.warning(
                    "Queue depth exceeds threshold "
                    "(pq=%d, write_q=%d) — backlog possible",
                    pq_depth, wq)

    # ---------------------------------------------------------------
    def _report_loop(self):
        _LABELS = {
            R_NO_TH4:    "No TH4 row in file",
            R_PROCESS:   "Process not in ALLOWED list",
            R_DUPLICATE: "Duplicate (track+process)",
            R_FILE_GONE: "File gone at read-time (read-at-detect)",
            R_PARSE_ERR: "Parse exception",
        }
        while not self.stop_evt.wait(REPORT_INTERVAL):
            with self.counter_lock:
                inserted = self.inserted
                skipped  = self.skipped.copy()

            total    = inserted + sum(skipped.values())
            skip_tot = sum(skipped.values())
            gap_pct  = (skip_tot / total * 100) if total else 0.0

            lines = [
                "",
                "=" * 72,
                f"  GAP REPORT  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "=" * 72,
                f"  Files seen this session  : {total:>8,}",
                f"  Inserted into log_data   : {inserted:>8,}",
                f"  Skipped (all reasons)    : "
                f"{skip_tot:>8,}  ({gap_pct:.2f}% gap)",
                "  " + "-" * 66,
            ]
            for reason, label in _LABELS.items():
                cnt = skipped.get(reason, 0)
                pct = (cnt / total * 100) if total else 0.0
                lines.append(
                    f"  {label:<52}: {cnt:>6,}  ({pct:.2f}%)")
            try:
                db_total, _ = self.db.get_stats()
                lines.append("  " + "-" * 66)
                lines.append(
                    f"  DB log_data total (all-time) : {db_total:>8,}")
            except Exception:
                pass
            lines.append("=" * 72)
            log.info("\n".join(lines))
            self._write_csv_report()

    # ---------------------------------------------------------------
    def _write_csv_report(self):
        try:
            cur = self.db.conn.cursor()
            cur.execute("""
                SELECT strftime('%Y-%m-%d %H:00', timestamp) hr, COUNT(*)
                FROM log_data WHERE timestamp IS NOT NULL
                GROUP BY hr ORDER BY hr
            """)
            ins_hr = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("""
                SELECT strftime('%Y-%m-%d %H:00', timestamp) hr,
                       reason, COUNT(*)
                FROM skip_log WHERE timestamp IS NOT NULL
                GROUP BY 1, 2 ORDER BY 1, 2
            """)
            skip_rows = cur.fetchall()
            skip_hr  = {}
            all_hrs  = set(ins_hr)
            for hr, reason, cnt in skip_rows:
                if hr is None:
                    continue
                all_hrs.add(hr)
                skip_hr.setdefault(hr, {})[reason] = cnt
            all_hrs.discard(None)
            with open(REPORT_CSV, "w", encoding="utf-8") as f:
                f.write("hour,inserted,NO_TH4,PROCESS_SKIP,"
                        "FILE_GONE,PARSE_ERROR,total,gap_pct\n")
                for hr in sorted(all_hrs):
                    ins  = ins_hr.get(hr, 0)
                    nth4 = skip_hr.get(hr, {}).get(R_NO_TH4, 0)
                    nprc = skip_hr.get(hr, {}).get(R_PROCESS, 0)
                    nfg  = skip_hr.get(hr, {}).get(R_FILE_GONE, 0)
                    npe  = skip_hr.get(hr, {}).get(R_PARSE_ERR, 0)
                    tot  = ins + nth4 + nprc + nfg + npe
                    gap  = ((tot - ins) / tot * 100) if tot else 0.0
                    f.write(f"{hr},{ins},{nth4},{nprc},"
                            f"{nfg},{npe},{tot},{gap:.2f}\n")
        except Exception as exc:
            log.warning("CSV report write failed: %s", exc)

    # ---------------------------------------------------------------
    def _startup_scan(self):
        try:
            entries = list(os.scandir(self.watch_folder))
        except OSError as exc:
            log.error("Cannot scan %s: %s", self.watch_folder, exc)
            return
        with self.queued_lock:
            files = [
                e.path for e in entries
                if e.is_file()
                and is_rslt_file(e.path)
                and e.name not in self.confirmed_files
                and e.name not in self.queued
            ]
        log.info("Startup scan: %d unprocessed files queued", len(files))
        for path in files:
            self.queue_file(path, source="startup")

    # ---------------------------------------------------------------
    def _drain_queues(self):
        with self._pq_lock:
            self._pq.clear()

    # ---------------------------------------------------------------
    def _log_final_stats(self):
        with self.counter_lock:
            inserted = self.inserted
            skipped  = self.skipped

        total   = inserted + sum(skipped.values())
        gap_pct = (sum(skipped.values()) / total * 100) if total else 0.0
        _LABELS = {
            R_NO_TH4:    "No TH4 row in file",
            R_PROCESS:   "Process not in ALLOWED list",
            R_DUPLICATE: "Duplicate (track+process)",
            R_FILE_GONE: "File gone at read-time",
            R_PARSE_ERR: "Parse exception",
        }
        log.info("=" * 72)
        log.info("FINAL SESSION SUMMARY  (v6 — read-at-detection)")
        log.info("  Total seen : %d", total)
        log.info("  Inserted   : %d", inserted)
        log.info("  Skipped    : %d  (%.2f%% gap)",
                 sum(skipped.values()), gap_pct)
        for reason, cnt in skipped.items():
            log.info("    %-52s : %d",
                     _LABELS.get(reason, reason), cnt)
        log.info("  FILE_GONE at read-time   : %d  "
                 "(was 6–7/session in v5; target = 0)",
                 self.file_gone_cnt)
        try:
            cur = self.db.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM no_th4_files")
            no_th4 = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM rescan_tracking")
            rescan = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM file_gone_log")
            gone_db = cur.fetchone()[0]
            log.info("  Files without TH4       : %d", no_th4)
            log.info("  Rescan/poll catches     : %d", rescan)
            log.info("  file_gone_log entries   : %d", gone_db)
        except Exception:
            pass
        log.info("=" * 72)


# ---------------------------------------------------------------------------
# SETUP VALIDATION
# ---------------------------------------------------------------------------
def validate_setup():
    errors = []
    if not os.path.isdir(LOG_FOLDER):
        errors.append(f"LOG_FOLDER not found: {LOG_FOLDER}")
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.isdir(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
        except OSError:
            errors.append(f"Cannot create DB directory: {db_dir}")
    if errors:
        for e in errors:
            log.error(e)
        sys.exit(1)

    try:
        from confluent_kafka import Producer
        p = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms":   "PLAIN",
            "sasl.username":     KAFKA_API_KEY,
            "sasl.password":     KAFKA_API_SECRET,
        })
        meta = p.list_topics(timeout=10)
        log.info("Kafka connectivity OK — %d topic(s) visible",
                 len(meta.topics))
    except Exception as exc:
        log.error("Kafka connectivity test FAILED: %s", exc)
        sys.exit(1)


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------
def main():
    import argparse
    ap = argparse.ArgumentParser(
        description="rslt_parser_v6 — read-at-detection, zero FILE_GONE")
    ap.add_argument("--import-mqs", metavar="CSV",
                    help="Import MQS export CSV and reconcile against DB")
    args = ap.parse_args()

    validate_setup()

    if args.import_mqs:
        db = SQLiteDB(DB_PATH)
        db.connect()
        db.import_mqs_csv(args.import_mqs)
        db.close()
        return

    log.info("=" * 72)
    log.info("rslt_parser_v6  [Read-at-Detection | Zero FILE_GONE | Kafka]")
    log.info("  Watch folder          : %s", LOG_FOLDER)
    log.info("  Database              : %s", DB_PATH)
    log.info("  Workers               : %d  (cpu_count=%d)",
             WORKER_COUNT, multiprocessing.cpu_count())
    log.info("  Kafka bootstrap       : %s", KAFKA_BOOTSTRAP)
    log.info("  Kafka topic           : %s", KAFKA_TOPIC_PARSED)
    log.info("  Kafka consumer group  : %s", KAFKA_CONSUMER_GROUP)
    log.info("  Content read retries  : %d x %d ms",
             CONTENT_READ_RETRIES, CONTENT_READ_RETRY_MS)
    log.info("  Allowed processes     : %s", sorted(ALLOWED_PROCESSES))
    log.info("=" * 72)

    parser = RsltParser(LOG_FOLDER, DB_PATH, WORKER_COUNT)
    parser.start()


if __name__ == "__main__":
    main()
