from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import sqlite3
import threading
import time

app = Flask(__name__)

conn = sqlite3.connect("brews.db", check_same_thread=False)


def init_db():
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS brews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brew_id TEXT NOT NULL,
                timestamp DATETIME NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS brews_hist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brew_id TEXT NOT NULL,
                count INT NOT NULL,
                timestamp DATETIME NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS brews_hist_meta (
                brew_id TEXT PRIMARY KEY,
                timestamp DATETIME NOT NULL
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_brew_id_timestamp ON brews (brew_id, timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_brew_id_timestamp_meta ON brews_hist_meta (brew_id, timestamp)")
        conn.commit()
    except:
        pass


init_db()

@app.route('/brew/<brew_id>', methods=['POST'])
def add_brew(brew_id):
    timestamp = datetime.now().isoformat()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO brews (brew_id, timestamp) VALUES (?, ?)", (brew_id, timestamp))
    cursor.execute("SELECT timestamp FROM brews_hist_meta WHERE brew_id = ?", (brew_id,))
    if cursor.fetchone() is None:
        cursor.execute("INSERT INTO brews_hist_meta (brew_id, timestamp) VALUES (?, ?)", (brew_id, timestamp))
    conn.commit()

    return "", 201


@app.route('/brew/<brew_id>', methods=['GET'])
def get_brews(brew_id):
    start_at = request.args.get('startAt', '1970-01-01T00:00:00')
    end_at = request.args.get('endAt', datetime.now().isoformat())
    limit = int(request.args.get('limit', 100))

    cursor = conn.cursor()
    query = """
        SELECT timestamp FROM brews 
        WHERE brew_id = ? AND timestamp > ? AND timestamp <= ? 
        ORDER BY timestamp ASC 
        LIMIT ?
    """
    cursor.execute(query, (brew_id, start_at, end_at, limit))
    rows = cursor.fetchall()

    timestamps = [row[0] for row in rows]
    last_timestamp = timestamps[-1] if timestamps else None

    return jsonify({"data": timestamps, "last": last_timestamp}), 200


def periodic_task():
    while True:
        time.sleep(600)

        with sqlite3.connect("brews.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT brew_id, timestamp FROM brews_hist_meta")
            rows = cursor.fetchall()

            for row in rows:
                brew_id, last_timestamp = row
                start_time = datetime.fromisoformat(last_timestamp)
                end_time = start_time + timedelta(minutes=10)

                cursor.execute("""
                    SELECT COUNT(*) FROM brews
                    WHERE brew_id = ? AND timestamp > ? AND timestamp <= ?
                """, (brew_id, start_time.isoformat(), end_time.isoformat()))

                count = cursor.fetchone()[0]

                cursor.execute("INSERT INTO brews_hist (brew_id, count, timestamp) VALUES (?, ?, ?)",
                               (brew_id, count, end_time.isoformat()))

                cursor.execute("UPDATE brews_hist_meta SET timestamp = ? WHERE brew_id = ?",
                               (end_time.isoformat(), brew_id))

                print(f'count from {brew_id}: {count}')

            conn.commit()


# 주기적인 작업을 별도의 스레드로 실행
thread = threading.Thread(target=periodic_task, daemon=True)
thread.start()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
