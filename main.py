from flask import Flask, jsonify
from flask_cors import CORS
from datetime import datetime, timedelta
import sqlite3
import threading
import time


app = Flask(__name__,
            static_url_path='',
            static_folder='web/static',
            template_folder='web/templates')
CORS(app)

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
    cursor.execute("SELECT timestamp FROM brews_hist_meta WHERE brew_id = ?", (brew_id,))
    if cursor.fetchone() is None:
        cursor.execute("INSERT INTO brews_hist_meta (brew_id, timestamp) VALUES (?, ?)", (brew_id, timestamp))
    conn.commit()

    return "", 201

@app.route('/brew/<brew_id>/data', methods=['POST'])
def add_brew_data(brew_id):
    timestamp = datetime.now().isoformat()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO brews (brew_id, timestamp) VALUES (?, ?)", (brew_id, timestamp))
    conn.commit()

    return "", 201


@app.route('/brew/<brew_id>', methods=['GET'])
def get_brews(brew_id):

    cursor = conn.cursor()
    query = """
        SELECT count, timestamp FROM brews_hist
        WHERE brew_id = ?
        ORDER BY timestamp ASC
    """
    cursor.execute(query, (brew_id,))
    rows = cursor.fetchall()

    data = [row[0] for row in rows]
    first = rows[0][1] if rows else None

    return jsonify({"data": data, "first": first}), 200


def periodic_task():
    while True:
        time.sleep(600)

        with sqlite3.connect("brews.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT brew_id, timestamp FROM brews_hist_meta")
            rows = cursor.fetchall()
            now = datetime.now()

            for row in rows:
                brew_id, last_timestamp = row
                start_time = datetime.fromisoformat(last_timestamp)
                end_time = start_time + timedelta(minutes=10)
                while end_time <= now:

                    cursor.execute("""
                        SELECT COUNT(*) FROM brews
                        WHERE brew_id = ? AND timestamp > ? AND timestamp <= ?
                    """, (brew_id, start_time.isoformat(), end_time.isoformat()))

                    count = cursor.fetchone()[0]

                    cursor.execute("INSERT INTO brews_hist (brew_id, count, timestamp) VALUES (?, ?, ?)",
                                   (brew_id, count, end_time.isoformat()))

                    cursor.execute("UPDATE brews_hist_meta SET timestamp = ? WHERE brew_id = ?",
                                   (end_time.isoformat(), brew_id))

                    print(f'count from {brew_id} until {end_time}: {count}')
                    end_time = end_time + timedelta(minutes=10)

            conn.commit()


thread = threading.Thread(target=periodic_task, daemon=True)
thread.start()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
