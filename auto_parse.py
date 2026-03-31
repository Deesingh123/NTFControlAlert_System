"""
rslt_parser_optimized.py  — Zero-skip parser for 2-minute file lifetime
======================================================================
FINAL OPTIMISATIONS:
- No file copying → parses directly from Q drive
- SQLite with WAL + forced checkpoint every 30 sec → data visible quickly
- Removes duplicate blocking → allows multiple entries for same track_id+process
- Tracks missing TH4 files in separate table
- Tracks rescan‑found files to measure watchdog coverage
- Fixed CSV report bug (handles None timestamps)
- Duplicate file processing eliminated (keeps file in 'queued' until commit)
- Robust error handling – threads never exit silently
- Health‑check thread monitors queue sizes and logs stalls
- Failed testcode extracted from column 14 (as per TH4 spec)
"""

import os
import re
import sys
import time
import queue
import sqlite3
import threading
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
LOG_FOLDER      = r"Q:\quality_data\test_results"
DB_PATH         = r"E:\Factor\rslt_data.db"
LOG_FILE        = r"E:\Factor\rslt_parser.log"
REPORT_CSV      = r"E:\Factor\gap_report.csv"

WORKER_COUNT    = 8
DB_BATCH_SIZE   = 50
FLUSH_INTERVAL  = 30      # seconds (forces WAL checkpoint)
RETRY_DELAY     = 1
MAX_RETRIES     = 2
RESCAN_INTERVAL = 10
REPORT_INTERVAL = 30
HEALTH_INTERVAL = 30      # seconds to log queue sizes
MAX_QUEUE_DEPTH = 200     # warning threshold

ALLOWED_PROCESSES = frozenset({
    "L2AR", "L2VISION", "UCT", "FOD", "DepthVal", "DepthCal"
})

# Skip reason codes
R_NO_TH4    = "NO_TH4"
R_PROCESS   = "PROCESS_SKIP"
R_DUPLICATE = "DUPLICATE"       # kept for compatibility, but we no longer use it
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
# SQLITE DATABASE MANAGER (with forced WAL checkpoint)
# ---------------------------------------------------------------------------
class SQLiteDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self._lock = threading.Lock()
        self._checkpoint_timer = None
        self._stop_checkpoint = threading.Event()

    def connect(self):
        """Open SQLite connection with optimal settings for real-time"""
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # CRITICAL: WAL mode for concurrent reads/writes
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")      # Balance speed vs safety
        conn.execute("PRAGMA cache_size=-10000")       # 10MB cache
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=268435456")     # 256MB mmap

        self.conn = conn
        self._init_tables()

        # Start WAL checkpoint thread
        self._start_checkpoint_thread()

        return conn

    def _init_tables(self):
        """Create tables if they don't exist"""
        cur = self.conn.cursor()

        # Main log table – no unique constraint on (track_id, process)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS log_data (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                process         TEXT,
                model           TEXT,
                line            TEXT,
                station         TEXT,
                status          TEXT,
                failed_testcode TEXT,
                track_id        TEXT,
                date            DATE,
                time            TIME,
                timestamp       TIMESTAMP
            )
        """)
        # Index for faster queries, but no unique constraint
        cur.execute("CREATE INDEX IF NOT EXISTS idx_track_process ON log_data (track_id, process)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON log_data (timestamp)")

        # Processed files (prevents re‑processing)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                filename       TEXT PRIMARY KEY,
                processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Skip log (kept for compatibility)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS skip_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                filename   TEXT,
                reason     TEXT,
                process    TEXT,
                track_id   TEXT,
                timestamp  TIMESTAMP,
                logged_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_skip_reason ON skip_log (reason)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_skip_ts ON skip_log (timestamp)")

        # New table: files without TH4 row
        cur.execute("""
            CREATE TABLE IF NOT EXISTS no_th4_files (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                filename    TEXT NOT NULL,
                track_id    TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # New table: files that were missed by watchdog but caught by rescan
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rescan_tracking (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                filename      TEXT NOT NULL,
                track_id      TEXT,
                process       TEXT,
                detected_at   TIMESTAMP,   -- when rescan found it
                inserted_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.commit()
        log.info("SQLite tables verified (WAL mode, duplicate tracking enabled)")

    def _start_checkpoint_thread(self):
        """Background thread that forces WAL checkpoint every FLUSH_INTERVAL"""
        def checkpoint_loop():
            while not self._stop_checkpoint.is_set():
                time.sleep(FLUSH_INTERVAL)
                try:
                    with self._lock:
                        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                        log.debug("WAL checkpoint completed")
                except Exception as e:
                    log.error("WAL checkpoint failed: %s", e)

        self._checkpoint_timer = threading.Thread(target=checkpoint_loop, daemon=True)
        self._checkpoint_timer.start()

    def execute_batch(self, insert_rows, skip_rows, processed_files,
                      processed_skips, rescan_rows=None):
        """
        Execute batch inserts in a single transaction.
        insert_rows: list of tuples for log_data
        skip_rows: list of tuples for skip_log
        processed_files: list of filenames to mark as processed (for inserted rows)
        processed_skips: list of filenames to mark as processed (for skipped rows)
        rescan_rows: optional list of (filename, track_id, process) for rescan_tracking
        """
        with self._lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                cur = self.conn.cursor()

                # Insert log_data (allow duplicates)
                if insert_rows:
                    cur.executemany("""
                        INSERT INTO log_data
                        (process, model, line, station, status, failed_testcode,
                         track_id, date, time, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, insert_rows)

                # Insert skip_log
                if skip_rows:
                    cur.executemany("""
                        INSERT INTO skip_log (filename, reason, process, track_id, timestamp)
                        VALUES (?, ?, ?, ?, ?)
                    """, skip_rows)

                # Mark processed files (both inserted and skipped)
                if processed_files:
                    cur.executemany("""
                        INSERT OR REPLACE INTO processed_files (filename, processed_time)
                        VALUES (?, CURRENT_TIMESTAMP)
                    """, [(f,) for f in processed_files])
                if processed_skips:
                    cur.executemany("""
                        INSERT OR REPLACE INTO processed_files (filename, processed_time)
                        VALUES (?, CURRENT_TIMESTAMP)
                    """, [(f,) for f in processed_skips])

                # Record rescan‑found files (if any)
                if rescan_rows:
                    cur.executemany("""
                        INSERT INTO rescan_tracking (filename, track_id, process, detected_at)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    """, rescan_rows)

                self.conn.commit()
                return len(insert_rows)

            except Exception as e:
                self.conn.rollback()
                log.error("Batch insert failed: %s", e)
                return 0

    def load_confirmed_files(self):
        """Load all processed filenames from DB"""
        cur = self.conn.cursor()
        cur.execute("SELECT filename FROM processed_files")
        return {row[0] for row in cur.fetchall()}

    def get_stats(self):
        """Get DB statistics"""
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM log_data")
        total = cur.fetchone()[0]

        cur.execute("SELECT reason, COUNT(*) FROM skip_log GROUP BY reason")
        skips = dict(cur.fetchall())

        return total, skips

    def close(self):
        """Close database connection"""
        self._stop_checkpoint.set()
        if self._checkpoint_timer:
            self._checkpoint_timer.join(timeout=5)

        if self.conn:
            # Final checkpoint
            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except:
                pass
            self.conn.close()
            log.info("Database closed, final WAL checkpoint done")

# ---------------------------------------------------------------------------
# FILENAME FILTER
# ---------------------------------------------------------------------------
def is_rslt_file(path: str) -> bool:
    return "rslt" in os.path.basename(path).lower()

def extract_track_id_from_filename(filename: str) -> str:
    """
    Extract track ID from filename.
    Patterns:
        ZA223HZJ6M_rslt_03-12-2026_15_22.40
        ZA223JF9BF.rslt.03-20-2026.17.36.48
    Returns empty string if not found.
    """
    # Remove extension and any trailing timestamp
    base = filename.replace('.rslt', '').replace('_rslt', '')
    # Split on first underscore or dot
    parts = re.split(r'[_.]', base)
    if parts:
        return parts[0].strip()
    return ""

# ---------------------------------------------------------------------------
# TESTCODE DETECTOR (kept for backward compatibility but not used for F status)
# ---------------------------------------------------------------------------
_GEO_SUFFIXES = frozenset({
    "INDIA","CHINA","USA","JAPAN","KOREA","GLOBAL",
    "EUROPE","LATAM","APAC","ROW","EMEA","DOMESTIC","EXPORT",
})
_SKIP_VALUES = frozenset({"(NULL)","NULL","NONE","*","N/A"})
_RE_HAS_DIGITS = re.compile(r"\d{2,}")
_RE_MODEL_STATION = re.compile(r"^[A-Z]+-[A-Z]")
_RE_KEYWORDS = re.compile(
    r"STATUS|RSSI|LEAKAGE|BLEMISH|CHARGE|WIFI|GPS|NFC|"
    r"SENSOR|AUDIO|CAMERA|LIGHT|BATTERY|DISPLAY|TOUCH|ANTENNA|"
    r"FIRMWARE|DOWNLOAD|COMPLETE|DETECT|VERIFY|CONNECT|ACTIVITY|"
    r"FREQ|OFFSET|CARRIER|NOISE|HEADSET|LEVEL|RESULT|REGION|"
    r"BARCODE|BOOT|CALIBR|FLASH|REBOOT|UNLOCK|LOCK|READ|WRITE"
)

def _is_testcode(raw: str) -> bool:
    """Return True only for genuine failed testcode strings (fallback only)"""
    val = raw.strip()
    if not val:                               return False
    if val.upper() in _SKIP_VALUES:           return False
    if val.startswith("REL."):                return False
    if "_" not in val:                        return False
    if any(c.islower() for c in val):         return False
    if _RE_MODEL_STATION.match(val):          return False
    segs = val.split(";")[0].strip().split("_")
    if segs[-1] in _GEO_SUFFIXES:            return False
    if len(segs) < 3:                         return False
    if not _RE_HAS_DIGITS.search(val) and not _RE_KEYWORDS.search(val):
        return False
    return True

# ---------------------------------------------------------------------------
# PARSE FUNCTION - DIRECT FILE READ (NO COPY)
# ---------------------------------------------------------------------------
def parse_rslt(filepath: str) -> dict | None:
    """
    Read TH4 line directly from file.
    Returns dict on success, None if file missing or no TH4 row.
    """
    try:
        with open(filepath, "r", errors="ignore") as fh:
            for raw_row in fh:
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
                line      = station.split("-")[0] if station else ""

                # Extract failed testcode for status F
                failed_tc = ""
                if status == "F":
                    # Directly take the 14th field (index 14) as per TH4 spec
                    if len(parts) > 14:
                        failed_tc = parts[14].strip()
                    # Fallback to scanning columns 10-15 if the above is empty
                    if not failed_tc:
                        for i in range(10, min(16, len(parts))):
                            if _is_testcode(parts[i]):
                                failed_tc = parts[i].strip()
                                break

                date_str = time_str = ts_std = None
                for fmt in ("%m/%d/%y %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
                    try:
                        dt = datetime.strptime(timestamp, fmt)
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

    except FileNotFoundError:
        # File already deleted (common in 2-minute window)
        log.debug("File vanished before parse: %s", os.path.basename(filepath))
        return None
    except OSError as exc:
        log.debug("Cannot read %s: %s", os.path.basename(filepath), exc)

    return None

# ---------------------------------------------------------------------------
# MAIN PARSER ENGINE
# ---------------------------------------------------------------------------
class RsltParser:
    def __init__(self, watch_folder, db_path, worker_count=8):
        self.watch_folder = watch_folder
        self.db = SQLiteDB(db_path)
        self.worker_count = worker_count

        # State
        self.confirmed_files = set()          # files already in DB (processed_files)
        self.queued = set()                   # files that are in queue or being processed
        self.queued_lock = threading.Lock()
        self.file_queue = queue.Queue()
        self.write_queue = queue.Queue()

        # Counters
        self.inserted = 0
        self.skipped = {}
        self.counter_lock = threading.Lock()

        # Control
        self.stop_evt = threading.Event()
        self.worker_threads = []   # keep references for monitoring (optional)

    def start(self):
        """Start all components"""
        # Connect to database
        self.db.connect()

        # Load already processed files
        self.confirmed_files = self.db.load_confirmed_files()

        total, skips = self.db.get_stats()
        log.info("DB state: %d rows | %d processed files | skips: %s",
                 total, len(self.confirmed_files), skips)

        # Start DB writer thread
        writer_thread = threading.Thread(target=self._db_writer_loop, daemon=True)
        writer_thread.start()

        # Start file processor threads (thread pool)
        with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
            # Submit file processing workers (they run indefinitely)
            for _ in range(self.worker_count):
                executor.submit(self._file_processor_loop)

            # Start watchdog
            observer = self._start_watchdog()

            # Start rescan thread
            rescan_thread = threading.Thread(target=self._rescan_loop, daemon=True)
            rescan_thread.start()

            # Start report thread
            report_thread = threading.Thread(target=self._report_loop, daemon=True)
            report_thread.start()

            # Start health‑check thread
            health_thread = threading.Thread(target=self._health_loop, daemon=True)
            health_thread.start()

            # Initial scan
            self._startup_scan()

            # Main loop - nothing heavy, just keep alive
            try:
                while not self.stop_evt.is_set():
                    time.sleep(0.5)
            except KeyboardInterrupt:
                log.info("Shutdown requested...")
            finally:
                self.stop_evt.set()
                observer.stop()
                observer.join()

                # Drain queues
                self._drain_queues()

                # Signal DB writer to stop
                self.write_queue.put(None)
                writer_thread.join(timeout=10)

                # Close database
                self.db.close()

                # Log final stats
                self._log_final_stats()

    def _start_watchdog(self):
        """Start file system monitor"""
        class RsltHandler(FileSystemEventHandler):
            def __init__(self, parser):
                self.parser = parser

            def on_created(self, event):
                if not event.is_directory and is_rslt_file(event.src_path):
                    self.parser.queue_file(event.src_path, source='watchdog')

        handler = RsltHandler(self)
        observer = Observer()
        observer.schedule(handler, self.watch_folder, recursive=False)
        observer.start()
        log.info("Watchdog started on %s", self.watch_folder)
        return observer

    def queue_file(self, filepath, source='watchdog'):
        """Add file to processing queue if not already queued or processed"""
        filename = os.path.basename(filepath)

        with self.queued_lock:
            if filename in self.confirmed_files or filename in self.queued:
                return
            self.queued.add(filename)

        self.file_queue.put((filepath, filename, 0, source))  # (path, name, retry, source)

    def _file_processor_loop(self):
        """Worker thread that processes files from queue. Never exits."""
        while not self.stop_evt.is_set():
            try:
                filepath, filename, retry_count, source = self.file_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                log.error("Unexpected error in file_queue.get: %s", e)
                time.sleep(1)
                continue

            try:
                # Parse directly from Q drive
                data = parse_rslt(filepath)

                if data is None:
                    # No TH4 row – record it in no_th4_files
                    track_id = extract_track_id_from_filename(filename)
                    self.write_queue.put(("NO_TH4", filename, track_id))
                    # The writer will remove from queued after commit
                    continue

                # Validate process
                if data["process"] not in ALLOWED_PROCESSES:
                    self.write_queue.put((
                        "SKIP", filename, R_PROCESS,
                        data["process"], data.get("track_id",""),
                        data.get("timestamp","") or ""
                    ))
                    continue

                # Log success (include failed testcode if present)
                failed_str = data["failed_tests"] if data["failed_tests"] else ""
                log.info("OK  track=%-22s proc=%-10s status=%s  failed='%.60s'",
                        data["track_id"], data["process"],
                        data["status"], failed_str)

                # Queue for DB write
                self.write_queue.put(("INSERT", data, filename, source))

            except Exception as e:
                log.error("Error processing %s: %s", filename, e)
                self.write_queue.put(("SKIP", filename, R_PARSE_ERR, "", "", ""))
                # Writer will handle removal from queued

            finally:
                self.file_queue.task_done()

    def _db_writer_loop(self):
        """Batch writer for SQLite. Never exits."""
        insert_buf = []      # list of tuples for log_data
        skip_buf = []        # list of tuples for skip_log
        processed_files = [] # filenames to mark as processed (inserted)
        processed_skips = [] # filenames to mark as processed (skipped)
        rescan_buf = []      # list of (filename, track_id, process) for rescan_tracking

        last_flush = time.time()

        while True:
            try:
                item = self.write_queue.get(timeout=1)
            except queue.Empty:
                # Flush if we have data or if FLUSH_INTERVAL elapsed
                if insert_buf or skip_buf or rescan_buf:
                    self._flush_batch(insert_buf, skip_buf, processed_files,
                                      processed_skips, rescan_buf)
                    insert_buf, skip_buf, processed_files, processed_skips, rescan_buf = [], [], [], [], []
                    last_flush = time.time()
                elif time.time() - last_flush > FLUSH_INTERVAL:
                    # Force WAL checkpoint even without data
                    try:
                        self.db.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                    except Exception as e:
                        log.error("WAL checkpoint error: %s", e)
                    last_flush = time.time()
                continue

            if item is None:  # Shutdown
                if insert_buf or skip_buf or rescan_buf:
                    self._flush_batch(insert_buf, skip_buf, processed_files,
                                      processed_skips, rescan_buf)
                break

            kind = item[0]

            try:
                if kind == "INSERT":
                    _, data, filename, source = item
                    # Convert dict to tuple for batch insert
                    insert_buf.append((
                        data["process"], data["model"], data["line"],
                        data["station"], data["status"], data["failed_tests"],
                        data["track_id"], data["date"], data["time"], data["timestamp"]
                    ))
                    processed_files.append(filename)

                    if source == 'rescan':
                        rescan_buf.append((filename, data["track_id"], data["process"]))

                    with self.counter_lock:
                        self.inserted += 1

                elif kind == "SKIP":
                    _, filename, reason, process, track_id, ts = item
                    skip_buf.append((filename, reason, process or "", track_id or "", ts or ""))
                    processed_skips.append(filename)

                    with self.counter_lock:
                        self.skipped[reason] = self.skipped.get(reason, 0) + 1

                elif kind == "NO_TH4":
                    _, filename, track_id = item
                    # Store in no_th4_files table
                    with self.db._lock:
                        cur = self.db.conn.cursor()
                        cur.execute("""
                            INSERT INTO no_th4_files (filename, track_id)
                            VALUES (?, ?)
                        """, (filename, track_id))
                        self.db.conn.commit()
                    # Also mark as processed to avoid retrying
                    processed_skips.append(filename)

                # Flush when batch size reached
                if len(insert_buf) + len(skip_buf) + len(rescan_buf) >= DB_BATCH_SIZE:
                    self._flush_batch(insert_buf, skip_buf, processed_files,
                                      processed_skips, rescan_buf)
                    insert_buf, skip_buf, processed_files, processed_skips, rescan_buf = [], [], [], [], []
                    last_flush = time.time()

            except Exception as e:
                log.error("Error in db_writer_loop processing item: %s", e)
                # Continue to next item (the item is lost, but we log it)
                continue

    def _flush_batch(self, insert_rows, skip_rows, processed_files,
                     processed_skips, rescan_rows):
        """Flush batch to database and remove files from queued set"""
        if not insert_rows and not skip_rows and not rescan_rows:
            return

        inserted = self.db.execute_batch(insert_rows, skip_rows,
                                         processed_files, processed_skips,
                                         rescan_rows)

        if inserted:
            log.info("DB commit: %d rows inserted", inserted)

        # Update confirmed files set and remove from queued
        with self.queued_lock:
            for fname in processed_files:
                self.confirmed_files.add(fname)
                self.queued.discard(fname)
            for fname in processed_skips:
                self.confirmed_files.add(fname)
                self.queued.discard(fname)

    def _rescan_loop(self):
        """Periodically rescan for missed files"""
        while not self.stop_evt.wait(RESCAN_INTERVAL):
            try:
                entries = list(os.scandir(self.watch_folder))
            except OSError as exc:
                log.warning("Rescan error: %s", exc)
                continue

            with self.queued_lock:
                new_files = []
                for entry in entries:
                    if not entry.is_file():
                        continue
                    if not is_rslt_file(entry.path):
                        continue
                    filename = entry.name
                    if filename not in self.confirmed_files and filename not in self.queued:
                        new_files.append(entry.path)

            if new_files:
                log.info("Rescan: %d missed file(s) found", len(new_files))
                for path in new_files:
                    self.queue_file(path, source='rescan')

    def _health_loop(self):
        """Periodically log queue sizes to detect stalls"""
        while not self.stop_evt.wait(HEALTH_INTERVAL):
            file_qsize = self.file_queue.qsize()
            write_qsize = self.write_queue.qsize()
            log.info("Health check: file_queue=%d, write_queue=%d, queued=%d",
                     file_qsize, write_qsize, len(self.queued))
            if file_qsize > MAX_QUEUE_DEPTH or write_qsize > MAX_QUEUE_DEPTH:
                log.warning("Queue depth exceeds threshold (%d/%d) – possible backlog",
                            file_qsize, write_qsize)

    def _report_loop(self):
        """Periodic gap report"""
        _REASON_LABELS = {
            R_NO_TH4:    "No TH4 row in file",
            R_PROCESS:   "Process not in ALLOWED list",
            R_DUPLICATE: "Duplicate (track+process)",
            R_FILE_GONE: "File deleted before read",
            R_PARSE_ERR: "Parse exception",
        }

        while not self.stop_evt.wait(REPORT_INTERVAL):
            with self.counter_lock:
                inserted = self.inserted
                skipped = self.skipped.copy()

            total = inserted + sum(skipped.values())
            skip_tot = sum(skipped.values())
            gap_pct = (skip_tot / total * 100) if total else 0.0

            lines = [
                "",
                "=" * 66,
                f"  GAP REPORT  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "=" * 66,
                f"  Files seen this session  : {total:>8,}",
                f"  Inserted into log_data   : {inserted:>8,}",
                f"  Skipped (all reasons)    : {skip_tot:>8,}  ({gap_pct:.1f}% gap)",
                "  " + "-" * 60,
            ]

            for reason, label in _REASON_LABELS.items():
                cnt = skipped.get(reason, 0)
                pct = (cnt / total * 100) if total else 0.0
                lines.append(f"  {label:<40}: {cnt:>6,}  ({pct:.1f}%)")

            # Add DB totals
            try:
                db_total, db_skips = self.db.get_stats()
                lines.append("  " + "-" * 60)
                lines.append(f"  DB log_data total (all-time) : {db_total:>8,}")
            except:
                pass

            lines.append("=" * 66)
            log.info("\n".join(lines))

            # Write CSV for monitoring
            self._write_csv_report()

    def _write_csv_report(self):
        """Write hourly CSV report, handling None timestamps"""
        try:
            cur = self.db.conn.cursor()

            # Hourly inserts – skip NULL timestamps
            cur.execute("""
                SELECT strftime('%Y-%m-%d %H:00', timestamp) as hr, COUNT(*)
                FROM log_data
                WHERE timestamp IS NOT NULL
                GROUP BY hr ORDER BY hr
            """)
            ins_hr = {r[0]: r[1] for r in cur.fetchall()}

            # Hourly skips – skip NULL timestamps
            cur.execute("""
                SELECT strftime('%Y-%m-%d %H:00', timestamp) as hr, reason, COUNT(*)
                FROM skip_log
                WHERE timestamp IS NOT NULL
                GROUP BY 1, 2 ORDER BY 1, 2
            """)
            skip_rows = cur.fetchall()

            skip_hr = {}
            all_hrs = set(ins_hr)
            for hr, reason, cnt in skip_rows:
                all_hrs.add(hr)
                if hr not in skip_hr:
                    skip_hr[hr] = {}
                skip_hr[hr][reason] = cnt

            with open(REPORT_CSV, "w", encoding="utf-8") as f:
                f.write("hour,inserted,NO_TH4,PROCESS_SKIP,DUPLICATE,PARSE_ERROR,total,gap_pct\n")
                for hr in sorted(all_hrs):
                    ins = ins_hr.get(hr, 0)
                    nth4 = skip_hr.get(hr, {}).get(R_NO_TH4, 0)
                    nprc = skip_hr.get(hr, {}).get(R_PROCESS, 0)
                    ndup = skip_hr.get(hr, {}).get(R_DUPLICATE, 0)
                    npe = skip_hr.get(hr, {}).get(R_PARSE_ERR, 0)
                    tot = ins + nth4 + nprc + ndup + npe
                    gap = ((tot - ins) / tot * 100) if tot else 0.0
                    f.write(f"{hr},{ins},{nth4},{nprc},{ndup},{npe},{tot},{gap:.1f}\n")
        except Exception as exc:
            log.warning("CSV report write failed: %s", exc)

    def _startup_scan(self):
        """Initial scan for unprocessed files"""
        try:
            entries = list(os.scandir(self.watch_folder))
        except OSError as exc:
            log.error("Cannot scan %s: %s", self.watch_folder, exc)
            return

        with self.queued_lock:
            files = [
                e.path for e in entries
                if e.is_file() and is_rslt_file(e.path)
                and e.name not in self.confirmed_files
                and e.name not in self.queued
            ]

        log.info("Startup scan: %d unprocessed files queued", len(files))
        for path in files:
            self.queue_file(path, source='startup')

    def _drain_queues(self):
        """Drain and process remaining items in queues (clean shutdown)"""
        # Process any pending file queue items (just discard them)
        while not self.file_queue.empty():
            try:
                self.file_queue.get_nowait()
                self.file_queue.task_done()
            except queue.Empty:
                break
        # The write_queue is handled by the writer thread

    def _log_final_stats(self):
        """Log final session statistics"""
        with self.counter_lock:
            inserted = self.inserted
            skipped = self.skipped

        total = inserted + sum(skipped.values())
        gap_pct = (sum(skipped.values()) / total * 100) if total else 0.0

        log.info("=" * 66)
        log.info("FINAL SESSION SUMMARY")
        log.info("  Total seen : %d", total)
        log.info("  Inserted   : %d", inserted)
        log.info("  Skipped    : %d  (%.1f%% gap)", sum(skipped.values()), gap_pct)

        _REASON_LABELS = {
            R_NO_TH4:    "No TH4 row in file",
            R_PROCESS:   "Process not in ALLOWED list",
            R_DUPLICATE: "Duplicate (track+process)",
            R_FILE_GONE: "File deleted before read",
            R_PARSE_ERR: "Parse exception",
        }

        for reason, cnt in skipped.items():
            label = _REASON_LABELS.get(reason, reason)
            log.info("    %-35s : %d", label, cnt)

        # Also show counts from new tables
        try:
            cur = self.db.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM no_th4_files")
            no_th4 = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM rescan_tracking")
            rescan = cur.fetchone()[0]
            log.info("  Files without TH4 (total)  : %d", no_th4)
            log.info("  Rescan‑found files (total) : %d", rescan)
        except Exception:
            pass

        log.info("=" * 66)

# ---------------------------------------------------------------------------
# VALIDATION
# ---------------------------------------------------------------------------
def validate_setup():
    """Check that required paths exist"""
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

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    validate_setup()

    log.info("=" * 66)
    log.info("rslt_parser_optimized  [Zero-skip + SQLite WAL + Edge Tracking]")
    log.info("  Watch folder : %s", LOG_FOLDER)
    log.info("  Database     : %s", DB_PATH)
    log.info("  Workers      : %d", WORKER_COUNT)
    log.info("  Flush interval: %d seconds", FLUSH_INTERVAL)
    log.info("=" * 66)

    parser = RsltParser(LOG_FOLDER, DB_PATH, WORKER_COUNT)
    parser.start()

if __name__ == "__main__":
    main()