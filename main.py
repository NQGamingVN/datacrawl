import requests
import json
import os
import time
from datetime import datetime
import psycopg2

API_URL = "https://wtx.tele68.com/v1/tx/sessions"
INTERVAL = 3500  # 3500 giây ~ 58 phút

# ====== KẾT NỐI DB ======
def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=5432
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
        resp = requests.get(API_URL, timeout=10)
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

# ====== CHẠY VÒNG LẶP ======
if __name__ == "__main__":
    init_db()
    while True:
        fetch_and_save()
        print(f"⏳ Chờ {INTERVAL} giây để lấy dữ liệu lần tiếp theo...\n")
        time.sleep(INTERVAL)
