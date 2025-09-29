import requests
import os
import time
import psycopg2
from datetime import datetime
from flask import Flask
import threading

API_URL = "https://wtx.tele68.com/v1/tx/sessions"
INTERVAL = 3500  # 3500 giây ~ 58 phút

# ====== KẾT NỐI DB ======
def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT", 5432)
    )

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        id BIGINT PRIMARY KEY,
        dice1 INT,
        dice2 INT,
        dice3 INT,
        point INT,
        result TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """)
    conn.commit()
    cur.close()
    conn.close()

# ====== LƯU DB ======
def save_to_db(new_sessions):
    conn = get_conn()
    cur = conn.cursor()
    for s in new_sessions:
        try:
            cur.execute("""
                INSERT INTO sessions (id, dice1, dice2, dice3, point, result)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (s["id"], s["dices"][0], s["dices"][1], s["dices"][2], s["point"], s["resultTruyenThong"]))
        except Exception as e:
            print("❌ Lỗi insert:", e)
    conn.commit()
    cur.close()
    conn.close()

# ====== FETCH & SAVE ======
def fetch_and_save():
    try:
        resp = requests.get(API_URL, timeout=60)
        data = resp.json()

        if "list" not in data:
            print("⚠️ API không trả về dữ liệu hợp lệ")
            return 0

        sessions = data["list"]
        sessions.sort(key=lambda x: x["id"])
        save_to_db(sessions)

        print(f"[{datetime.now()}] ✅ Đã lưu {len(sessions)} phiên "
              f"(ID {sessions[0]['id']} → {sessions[-1]['id']})")

        return len(sessions)

    except Exception as e:
        print(f"[{datetime.now()}] ❌ Lỗi khi fetch:", e)
        return 0

# ====== VÒNG LẶP ======
def loop_task():
    init_db()
    while True:
        fetch_and_save()
        print(f"⏳ Chờ {INTERVAL} giây...\n")
        time.sleep(INTERVAL)

# ====== FLASK WEB ======
app = Flask(__name__)

@app.route("/")
def home():
    return "App is running 🐢"

@app.route("/health")
def health():
    return "OK"

if __name__ == "__main__":
    # chạy loop_task trong thread riêng
    t = threading.Thread(target=loop_task, daemon=True)
    t.start()

    # chạy Flask server trên port Render cung cấp
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
