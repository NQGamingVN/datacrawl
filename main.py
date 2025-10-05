import requests
import os
import time
import psycopg2
from datetime import datetime, timedelta
from flask import Flask, render_template_string, Response, jsonify
import threading
import json
import zipfile
from io import BytesIO

API_URL = "https://wtx.tele68.com/v1/tx/sessions"
INTERVAL = 3600
RETRY_INTERVAL = 300
MAX_RETRIES = 5

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
    
    cur.execute("SELECT id FROM sessions ORDER BY id")
    all_ids = [row[0] for row in cur.fetchall()]
    
    if not all_ids:
        return []
    
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
        for i, chunk in enumerate(chunks[:10]):
            if chunk:
                stats['chunks_info'].append({
                    'name': f'data{i+1}',
                    'start': chunk[0],
                    'end': chunk[-1],
                    'count': len(chunk)
                })
        
        # Thời gian cập nhật
        stats['last_updated'] = datetime.now().strftime("%H:%M:%S %d/%m/%Y")
        next_fetch = datetime.now() + timedelta(hours=1)
        stats['next_fetch'] = next_fetch.strftime("%H:%M:%S")
        
    except Exception as e:
        print(f"❌ Lỗi khi thống kê: {e}")
        stats['error'] = str(e)
    finally:
        cur.close()
        conn.close()
    
    return stats

# ====== XUẤT FILE TOÀN BỘ ======
def export_full_txt():
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

def export_full_json():
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

# ====== XUẤT FILE THEO CHUỖI LIÊN TỤC ======
def export_continuous_chunks_txt():
    """Xuất nhiều file TXT theo chuỗi liên tục"""
    conn = get_conn()
    if not conn:
        return "❌ Lỗi kết nối database", 500
    
    try:
        chunks = get_continuous_chunks()
        
        if not chunks:
            return "❌ Không có dữ liệu để xuất", 404
        
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, chunk in enumerate(chunks):
                if len(chunk) > 0:
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
                    
                    content = ""
                    for row in rows:
                        issue_id, dice1, dice2, dice3, point, result_text = row
                        content += f"{issue_id}|{dice1}:{dice2}:{dice3}|{point}|{result_text}\n"
                    
                    filename = f"data{i+1}.txt"
                    zip_file.writestr(filename, content)
        
        zip_buffer.seek(0)
        
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
    """Xuất nhiều file JSON theo chuỗi liên tục"""
    conn = get_conn()
    if not conn:
        return "❌ Lỗi kết nối database", 500
    
    try:
        chunks = get_continuous_chunks()
        
        if not chunks:
            return "❌ Không có dữ liệu để xuất", 404
        
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, chunk in enumerate(chunks):
                if len(chunk) > 0:
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
                    
                    filename = f"data{i+1}.json"
                    zip_file.writestr(filename, json.dumps(data, ensure_ascii=False, indent=2))
        
        zip_buffer.seek(0)
        
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

# HTML Template - ĐÃ ADAPT TỪ VN58 SANG TX
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎲 Quản Lý Dữ Liệu TX</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        .header {
            background: rgba(255, 255, 255, 0.95);
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.2);
            text-align: center;
            margin-bottom: 30px;
        }
        .header h1 { color: #333; font-size: 2.5em; margin-bottom: 10px; }
        .header p { color: #666; font-size: 1.1em; }
        
        .card {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 25px;
            box-shadow: 0 5px 20px rgba(0, 0, 0, 0.1);
        }
        .card h2 { 
            color: #333; 
            margin-bottom: 20px; 
            border-bottom: 2px solid #667eea; 
            padding-bottom: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .stat-item {
            background: linear-gradient(135deg, #667eea, #764ba2);
            color: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }
        .stat-number { font-size: 2em; font-weight: bold; margin-bottom: 5px; }
        .stat-label { font-size: 0.9em; opacity: 0.9; }
        
        .session-item {
            background: #f8f9fa;
            border-left: 4px solid #667eea;
            padding: 15px;
            margin-bottom: 10px;
            border-radius: 5px;
        }
        .badge {
            display: inline-block;
            padding: 5px 10px;
            border-radius: 20px;
            font-size: 0.8em;
            font-weight: bold;
            margin-right: 10px;
        }
        .badge-success { background: #28a745; color: white; }
        .badge-warning { background: #ffc107; color: black; }
        .badge-danger { background: #dc3545; color: white; }
        .badge-info { background: #17a2b8; color: white; }
        
        .nav-tabs {
            display: flex;
            margin-bottom: 20px;
            background: white;
            border-radius: 10px;
            padding: 10px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
        }
        .nav-tab {
            padding: 10px 20px;
            margin-right: 10px;
            border-radius: 5px;
            cursor: pointer;
            transition: all 0.3s ease;
        }
        .nav-tab.active { background: #667eea; color: white; }
        
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        
        .btn {
            background: #667eea;
            color: white;
            padding: 10px 20px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            text-decoration: none;
            display: inline-block;
            margin: 5px;
            transition: background 0.3s ease;
        }
        .btn:hover { background: #764ba2; }
        .btn-refresh {
            background: #28a745;
            font-size: 0.9em;
            padding: 8px 15px;
        }
        .btn-success {
            background: #28a745;
        }
        
        .update-info {
            background: #e7f3ff;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 20px;
            text-align: center;
            border-left: 4px solid #2196F3;
        }
        
        .next-update {
            background: #fff3cd;
            padding: 10px 15px;
            border-radius: 10px;
            margin-top: 10px;
            display: inline-block;
        }
        
        .chunk-info {
            background: #f8f9fa;
            padding: 10px;
            border-radius: 5px;
            margin: 5px 0;
            border-left: 3px solid #28a745;
        }
        
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
            <p>Theo dõi và phân tích dữ liệu phiên chơi TX</p>
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

        <div class="nav-tabs">
            <div class="nav-tab active" onclick="showTab('dashboard')">📊 Dashboard</div>
            <div class="nav-tab" onclick="showTab('sessions')">🎯 Phiên gần đây</div>
            <div class="nav-tab" onclick="showTab('export')">📁 Xuất dữ liệu</div>
        </div>

        <!-- Dashboard Tab -->
        <div id="dashboard" class="tab-content active">
            <div class="card">
                <h2>
                    📈 Tổng quan hệ thống
                    <button class="btn btn-refresh" onclick="refreshData()">🔄 Làm mới</button>
                </h2>
                <div class="stats-grid">
                    <div class="stat-item">
                        <div class="stat-number">{{ stats.total_sessions }}</div>
                        <div class="stat-label">Tổng số phiên</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-number">{{ stats.continuous_chunks }}</div>
                        <div class="stat-label">Số chuỗi liên tục</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-number">{{ stats.duplicate_sessions|length }}</div>
                        <div class="stat-label">Phiên trùng lặp</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-number">{{ stats.last_session[0] if stats.last_session else 'N/A' }}</div>
                        <div class="stat-label">Phiên mới nhất</div>
                    </div>
                </div>
            </div>

            {% if stats.chunks_info %}
            <div class="card">
                <h2>🔗 Các chuỗi dữ liệu liên tục</h2>
                {% for chunk in stats.chunks_info %}
                <div class="chunk-info">
                    <strong>{{ chunk.name }}</strong>: {{ chunk.start }} → {{ chunk.end }} ({{ chunk.count }} phiên)
                </div>
                {% endfor %}
                {% if stats.continuous_chunks > 10 %}
                <p>... và {{ stats.continuous_chunks - 10 }} chuỗi khác</p>
                {% endif %}
            </div>
            {% endif %}

            <div class="card">
                <h2>📋 Thông tin phiên</h2>
                {% if stats.first_session %}
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                    <div>
                        <h3>Phiên đầu tiên</h3>
                        <div class="session-item">
                            ID: {{ stats.first_session[0] }}<br>
                            Xúc xắc: {{ stats.first_session[1] }}:{{ stats.first_session[2] }}:{{ stats.first_session[3] }}<br>
                            Điểm: {{ stats.first_session[4] }}<br>
                            Kết quả: <span class="badge badge-info">{{ stats.first_session[5] }}</span>
                        </div>
                    </div>
                    <div>
                        <h3>Phiên cuối cùng</h3>
                        <div class="session-item">
                            ID: {{ stats.last_session[0] }}<br>
                            Xúc xắc: {{ stats.last_session[1] }}:{{ stats.last_session[2] }}:{{ stats.last_session[3] }}<br>
                            Điểm: {{ stats.last_session[4] }}<br>
                            Kết quả: <span class="badge badge-info">{{ stats.last_session[5] }}</span>
                        </div>
                    </div>
                </div>
                {% endif %}
            </div>

            {% if stats.duplicate_sessions %}
            <div class="card">
                <h2>⚠️ Phiên trùng lặp</h2>
                {% for session_id, count in stats.duplicate_sessions %}
                <div class="session-item">
                    <span class="badge badge-warning">Trùng</span>
                    Phiên {{ session_id }} (xuất hiện {{ count }} lần)
                </div>
                {% endfor %}
            </div>
            {% endif %}

            {% if stats.missing_sessions %}
            <div class="card">
                <h2>🔍 Khoảng trống dữ liệu</h2>
                {% for start, end, count in stats.missing_sessions %}
                <div class="session-item">
                    <span class="badge badge-danger">Thiếu</span>
                    {% if start == end %}
                    Thiếu phiên: {{ start }}
                    {% else %}
                    Thiếu từ phiên {{ start }} đến {{ end }} ({{ count }} phiên)
                    {% endif %}
                </div>
                {% endfor %}
            </div>
            {% endif %}
        </div>

        <!-- Recent Sessions Tab -->
        <div id="sessions" class="tab-content">
            <div class="card">
                <h2>
                    🎯 20 Phiên gần đây nhất
                    <button class="btn btn-refresh" onclick="refreshData()">🔄 Làm mới</button>
                </h2>
                {% if stats.recent_sessions %}
                {% for session in stats.recent_sessions %}
                <div class="session-item">
                    <strong>Phiên {{ session[0] }}</strong><br>
                    Xúc xắc: {{ session[1] }}:{{ session[2] }}:{{ session[3] }} | 
                    Điểm: {{ session[4] }} | 
                    Kết quả: <span class="badge 
                        {% if 'THẮNG' in session[5] %}badge-success
                        {% elif 'THUA' in session[5] %}badge-danger
                        {% else %}badge-info{% endif %}">
                        {{ session[5] }}
                    </span>
                </div>
                {% endfor %}
                {% else %}
                <p>Chưa có dữ liệu phiên nào.</p>
                {% endif %}
            </div>
        </div>

        <!-- Export Tab -->
        <div id="export" class="tab-content">
            <div class="card">
                <h2>📁 Xuất dữ liệu</h2>
                
                <p><strong>📦 Xuất toàn bộ (1 file):</strong></p>
                <a href="/export/txt" class="btn">📄 Xuất file TXT</a>
                <a href="/export/json" class="btn">📋 Xuất file JSON</a>
                
                <p style="margin-top: 20px;"><strong>🔗 Xuất theo chuỗi liên tục (Khuyến nghị):</strong></p>
                <p><small>Dữ liệu được tách thành nhiều file data1, data2, data3... mỗi file là một chuỗi ID liên tục</small></p>
                <a href="/export/continuous-txt" class="btn btn-success">📁 TXT theo chuỗi liên tục</a>
                <a href="/export/continuous-json" class="btn btn-success">📁 JSON theo chuỗi liên tục</a>
                
                <a href="/api/data" class="btn">🔗 API JSON</a>
                
                <div style="margin-top: 20px; padding: 15px; background: #f8f9fa; border-radius: 5px;">
                    <h3>📊 Thống kê hiện tại</h3>
                    <p><strong>Tổng số phiên:</strong> {{ stats.total_sessions }}</p>
                    <p><strong>Số chuỗi liên tục:</strong> {{ stats.continuous_chunks }}</p>
                    <p><strong>Phiên đầu:</strong> {% if stats.first_session %}{{ stats.first_session[0] }}{% endif %}</p>
                    <p><strong>Phiên cuối:</strong> {% if stats.last_session %}{{ stats.last_session[0] }}{% endif %}</p>
                    <p><strong>Cập nhật:</strong> {{ stats.last_updated }}</p>
                </div>
            </div>
        </div>
    </div>

    <script>
        function showTab(tabName) {
            document.querySelectorAll('.tab-content').forEach(tab => {
                tab.classList.remove('active');
            });
            document.querySelectorAll('.nav-tab').forEach(tab => {
                tab.classList.remove('active');
            });
            
            document.getElementById(tabName).classList.add('active');
            event.target.classList.add('active');
        }

        function refreshData() {
            window.location.reload();
        }
    </script>
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
    return export_full_txt()

@app.route("/export/json")
def export_json():
    return export_full_json()

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

# ====== VÒNG LẶP CHÍNH ======
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
