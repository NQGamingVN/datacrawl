import requests
import os
import time
import psycopg2
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request, jsonify, Response
import threading
import json
import zipfile
from io import BytesIO

API_URL = "https://wtx.tele68.com/v1/tx/sessions"
INTERVAL = 3600
RETRY_INTERVAL = 300  # 5 phút giữa các lần retry
MAX_RETRIES = 5  # Tối đa 5 lần thử lại

# ====== KẾT NỐI DB ======
def get_conn():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("❌ DATABASE_URL chưa được set trong environment")
    if "sslmode" not in dsn:
        if "?" in dsn:
            dsn += "&sslmode=require"
        else:
            dsn += "?sslmode=require"

    retries = 5
    for i in range(retries):
        try:
            conn = psycopg2.connect(dsn)
            print("✅ Kết nối database thành công")
            return conn
        except Exception as e:
            print(f"❌ Kết nối DB thất bại ({i+1}/{retries}): {e}")
            time.sleep(5)
    raise Exception("Không thể kết nối database sau nhiều lần retry")

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
        result TEXT
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
            """, (s["id"], s["dices"][0], s["dices"][1], s["dices"][2],
                  s["point"], s["resultTruyenThong"]))
        except Exception as e:
            print("❌ Lỗi insert:", e)
    conn.commit()
    cur.close()
    conn.close()

# ====== FETCH & SAVE VỚI RETRY ======
def fetch_and_save_with_retry():
    """Fetch và save dữ liệu với retry mechanism"""
    for attempt in range(MAX_RETRIES):
        try:
            print(f"🔄 Bắt đầu fetch dữ liệu (lần {attempt + 1}/{MAX_RETRIES})...")
            
            resp = requests.get(API_URL, timeout=60)
            data = resp.json()

            if "list" not in data:
                print("⚠️ API không trả về dữ liệu hợp lệ")
                continue

            sessions = data["list"]
            sessions.sort(key=lambda x: x["id"])
            save_to_db(sessions)

            print(f"[{datetime.now()}] ✅ Fetch thành công! Đã lưu {len(sessions)} phiên "
                  f"(ID {sessions[0]['id']} → {sessions[-1]['id']})")
            return len(sessions)

        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi mạng/API lần {attempt + 1}/{MAX_RETRIES}: {e}")
        except Exception as e:
            print(f"❌ Lỗi không xác định lần {attempt + 1}/{MAX_RETRIES}: {e}")
        
        # Chờ trước khi thử lại (trừ lần cuối)
        if attempt < MAX_RETRIES - 1:
            print(f"⏳ Chờ {RETRY_INTERVAL} giây trước khi thử lại fetch...")
            time.sleep(RETRY_INTERVAL)
        else:
            print("🚫 Đã thử fetch tối đa số lần, chuyển sang chu kỳ tiếp theo")
    
    return 0

# ====== CHUỖI LIÊN TỤC ======
def get_continuous_chunks():
    """Lấy các chuỗi dữ liệu liên tục (không bị đứt)"""
    conn = get_conn()
    cur = conn.cursor()
    
    # Lấy tất cả ID và sắp xếp
    cur.execute("SELECT id FROM sessions ORDER BY id")
    all_ids = [row[0] for row in cur.fetchall()]
    
    if not all_ids:
        return []
    
    # Tìm các chuỗi liên tục
    chunks = []
    current_chunk = [all_ids[0]]
    
    for i in range(1, len(all_ids)):
        if all_ids[i] == all_ids[i-1] + 1:
            current_chunk.append(all_ids[i])
        else:
            chunks.append(current_chunk)
            current_chunk = [all_ids[i]]
    
    chunks.append(current_chunk)
    
    cur.close()
    conn.close()
    return chunks

# ====== STATISTICS FUNCTIONS ======
def get_statistics():
    """Lấy thống kê dữ liệu - LUÔN LÀM MỚI KHI GỌI"""
    conn = get_conn()
    if not conn:
        return None
    
    cur = conn.cursor()
    stats = {}
    
    try:
        # Tổng số phiên
        cur.execute("SELECT COUNT(*) FROM sessions")
        stats['total_sessions'] = cur.fetchone()[0]
        
        # Phiên đầu tiên và cuối cùng
        cur.execute("SELECT id, dice1, dice2, dice3, point, result FROM sessions ORDER BY id ASC LIMIT 1")
        stats['first_session'] = cur.fetchone()
        
        cur.execute("SELECT id, dice1, dice2, dice3, point, result FROM sessions ORDER BY id DESC LIMIT 1")
        stats['last_session'] = cur.fetchone()
        
        # Phiên trùng
        cur.execute("SELECT id, COUNT(*) FROM sessions GROUP BY id HAVING COUNT(*) > 1")
        stats['duplicate_sessions'] = cur.fetchall()
        
        # Phiên thiếu
        cur.execute("""
            WITH gaps AS (
                SELECT id, LAG(id) OVER (ORDER BY id) as prev_id
                FROM sessions
            )
            SELECT prev_id + 1 as missing_start, id - 1 as missing_end,
                   id - prev_id - 1 as missing_count
            FROM gaps WHERE id - prev_id > 1
            ORDER BY missing_start
        """)
        stats['missing_sessions'] = cur.fetchall()
        
        # Dữ liệu mới nhất (20 phiên)
        cur.execute("""
            SELECT id, dice1, dice2, dice3, point, result
            FROM sessions ORDER BY id DESC LIMIT 20
        """)
        stats['recent_sessions'] = cur.fetchall()
        
        # Thống kê chuỗi liên tục
        chunks = get_continuous_chunks()
        stats['continuous_chunks'] = len(chunks)
        stats['chunks_info'] = []
        for i, chunk in enumerate(chunks[:10]):  # Hiển thị 10 chuỗi đầu
            if chunk:
                stats['chunks_info'].append({
                    'name': f'data{i+1}',
                    'start': chunk[0],
                    'end': chunk[-1],
                    'count': len(chunk)
                })
        
        # Thời gian cập nhật
        stats['last_updated'] = datetime.now().strftime("%H:%M:%S %d/%m/%Y")
        
        # Tính thời gian fetch tiếp theo (1 tiếng sau)
        next_fetch = datetime.now() + timedelta(hours=1)
        stats['next_fetch'] = next_fetch.strftime("%H:%M:%S")
        
    except Exception as e:
        print(f"❌ Lỗi khi thống kê: {e}")
        stats['error'] = str(e)
    finally:
        cur.close()
        conn.close()
    
    return stats

# ====== XUẤT FILE THEO CHUỖI LIÊN TỤC ======
def export_continuous_chunks_txt():
    """Xuất nhiều file TXT theo chuỗi liên tục - đặt tên data1, data2..."""
    conn = get_conn()
    if not conn:
        return "❌ Lỗi kết nối database", 500
    
    try:
        # Lấy các chuỗi liên tục
        chunks = get_continuous_chunks()
        
        if not chunks:
            return "❌ Không có dữ liệu để xuất", 404
        
        # Tạo ZIP chứa tất cả file
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, chunk in enumerate(chunks):
                if len(chunk) > 0:
                    # Lấy dữ liệu cho chunk này
                    cur = conn.cursor()
                    placeholders = ','.join(['%s'] * len(chunk))
                    cur.execute(f"""
                        SELECT id, dice1, dice2, dice3, point, result 
                        FROM sessions 
                        WHERE id IN ({placeholders}) 
                        ORDER BY id
                    """, chunk)
                    rows = cur.fetchall()
                    cur.close()
                    
                    # Tạo nội dung file
                    content = ""
                    for row in rows:
                        issue_id, dice1, dice2, dice3, point, result_text = row
                        content += f"{issue_id}|{dice1}:{dice2}:{dice3}|{point}|{result_text}\n"
                    
                    # ĐẶT TÊN FILE: data1, data2, data3...
                    filename = f"data{i+1}.txt"
                    
                    # Thêm vào ZIP
                    zip_file.writestr(filename, content)
        
        zip_buffer.seek(0)
        
        # Trả về file ZIP
        return Response(
            zip_buffer.getvalue(),
            mimetype="application/zip",
            headers={"Content-Disposition": "attachment;filename=tx_data_continuous.zip"}
        )
        
    except Exception as e:
        return f"❌ Lỗi khi xuất file: {e}", 500
    finally:
        conn.close()

def export_continuous_chunks_json():
    """Xuất nhiều file JSON theo chuỗi liên tục - đặt tên data1, data2..."""
    conn = get_conn()
    if not conn:
        return "❌ Lỗi kết nối database", 500
    
    try:
        # Lấy các chuỗi liên tục
        chunks = get_continuous_chunks()
        
        if not chunks:
            return "❌ Không có dữ liệu để xuất", 404
        
        # Tạo ZIP chứa tất cả file
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, chunk in enumerate(chunks):
                if len(chunk) > 0:
                    # Lấy dữ liệu cho chunk này
                    cur = conn.cursor()
                    placeholders = ','.join(['%s'] * len(chunk))
                    cur.execute(f"""
                        SELECT id, dice1, dice2, dice3, point, result 
                        FROM sessions 
                        WHERE id IN ({placeholders}) 
                        ORDER BY id
                    """, chunk)
                    rows = cur.fetchall()
                    cur.close()
                    
                    # Chuyển đổi sang JSON
                    data = []
                    for row in rows:
                        issue_id, dice1, dice2, dice3, point, result_text = row
                        data.append({
                            "id": issue_id,
                            "dice1": dice1,
                            "dice2": dice2,
                            "dice3": dice3,
                            "point": point,
                            "result": result_text
                        })
                    
                    # ĐẶT TÊN FILE: data1, data2, data3...
                    filename = f"data{i+1}.json"
                    
                    # Thêm vào ZIP
                    zip_file.writestr(filename, json.dumps(data, ensure_ascii=False, indent=2))
        
        zip_buffer.seek(0)
        
        # Trả về file ZIP
        return Response(
            zip_buffer.getvalue(),
            mimetype="application/zip",
            headers={"Content-Disposition": "attachment;filename=tx_data_continuous.zip"}
        )
        
    except Exception as e:
        return f"❌ Lỗi khi xuất file: {e}", 500
    finally:
        conn.close()

# ====== FLASK WEB APP ======
app = Flask(__name__)

# HTML Template - CẬP NHẬT THÊM RETRY INFO
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎲 Quản Lý Dữ Liệu TX</title>
    <style>
        /* CSS giữ nguyên */
        .retry-info {
            background: #d4edda;
            padding: 10px;
            border-radius: 5px;
            margin: 10px 0;
            border-left: 4px solid #28a745;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎲 Quản Lý Dữ Liệu TX</h1>
            <p>Theo dõi và phân tích dữ liệu phiên chơi tự động</p>
        </div>

        <div class="update-info">
            <div>
                📊 <strong>Dữ liệu thời gian thực</strong> - Cập nhật lúc: <strong>{{ stats.last_updated }}</strong>
            </div>
            <div class="next-update">
                ⏰ Lần thu thập tiếp theo: <strong>{{ stats.next_fetch }}</strong>
                <br>
                <small>Dữ liệu được thu thập tự động mỗi giờ</small>
            </div>
            <div class="retry-info">
                🔄 <strong>Auto Retry:</strong> Tối đa 5 lần thử lại (mỗi 5 phút) khi có lỗi
            </div>
        </div>

        <!-- Các phần còn lại giữ nguyên -->
    </div>
</body>
</html>
'''

@app.route("/")
def home():
    stats = get_statistics()
    return render_template_string(HTML_TEMPLATE, stats=stats)

@app.route("/health")
def health():
    return "OK"

@app.route("/export/txt")
def export_txt():
    """Xuất file TXT toàn bộ"""
    conn = get_conn()
    if not conn:
        return "❌ Lỗi kết nối database", 500
    
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, dice1, dice2, dice3, point, result FROM sessions ORDER BY id ASC")
        rows = cur.fetchall()
        
        if not rows:
            return "❌ Không có dữ liệu để xuất", 404
        
        filename = f"tx_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        content = ""
        for row in rows:
            issue_id, dice1, dice2, dice3, point, result_text = row
            content += f"{issue_id}|{dice1}:{dice2}:{dice3}|{point}|{result_text}\n"
        
        return Response(
            content,
            mimetype="text/plain",
            headers={"Content-Disposition": f"attachment;filename={filename}"}
        )
        
    except Exception as e:
        return f"❌ Lỗi khi xuất file TXT: {e}", 500
    finally:
        cur.close()
        conn.close()

@app.route("/export/json")
def export_json():
    """Xuất file JSON toàn bộ"""
    conn = get_conn()
    if not conn:
        return "❌ Lỗi kết nối database", 500
    
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, dice1, dice2, dice3, point, result FROM sessions ORDER BY id ASC")
        rows = cur.fetchall()
        
        if not rows:
            return "❌ Không có dữ liệu để xuất", 404
        
        data = []
        for row in rows:
            issue_id, dice1, dice2, dice3, point, result_text = row
            data.append({
                "id": issue_id,
                "dice1": dice1,
                "dice2": dice2,
                "dice3": dice3,
                "point": point,
                "result": result_text
            })
        
        filename = f"tx_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        return Response(
            json.dumps(data, ensure_ascii=False, indent=2),
            mimetype="application/json",
            headers={"Content-Disposition": f"attachment;filename={filename}"}
        )
        
    except Exception as e:
        return f"❌ Lỗi khi xuất file JSON: {e}", 500
    finally:
        cur.close()
        conn.close()

@app.route("/export/continuous-txt")
def export_continuous_txt():
    return export_continuous_chunks_txt()

@app.route("/export/continuous-json")
def export_continuous_json():
    return export_continuous_chunks_json()

@app.route("/api/data")
def api_data():
    stats = get_statistics()
    return jsonify(stats)

# ====== VÒNG LẶP FETCH DỮ LIỆU VỚI RETRY ======
def loop_task():
    while True:
        try:
            init_db()
            saved_count = fetch_and_save_with_retry()
            if saved_count > 0:
                print(f"✅ Đã thu thập thành công {saved_count} phiên mới")
            else:
                print("⚠️ Không có phiên mới nào được thu thập")
        except Exception as e:
            print(f"[{datetime.now()}] ⚠️ Lỗi trong loop_task: {e}")
        print(f"⏳ Chờ {INTERVAL} giây...\n")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    t = threading.Thread(target=loop_task, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
