import os
import time
import requests
import psycopg2
import json
import zipfile
from datetime import datetime, timedelta
from flask import Flask, render_template_string, Response
import threading
from io import BytesIO

# ===== CONFIG =====
API_URL = "https://www.vn58q.bet/api/minigame/games/PK3_60S/history100"
LOGIN_URL = "https://www.vn58q.bet/api/account/login"

USERNAME = "quangnormal"
PASSWORD = "12345abC_"
DEVICE_ID = "b01c2bec8afd532578f3b73ae748082d"

INTERVAL = 3600  # 1 giờ
RETRY_INTERVAL = 300  # 5 phút giữa các lần retry
MAX_RETRIES = 5  # Tối đa 5 lần thử lại

DATABASE_URL = "postgresql://postgres.yqtvaxgthwqjegjouxko:12345abC_MatKhau@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres?sslmode=require"

# ===== DB helper =====
def get_conn():
    retries = 5
    for i in range(retries):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            return conn
        except Exception as e:
            print(f"❌ Kết nối DB thất bại ({i+1}/{retries}): {e}", flush=True)
            time.sleep(5)
    raise Exception("Không thể kết nối DB")

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sessions_new (
        issue_id TEXT PRIMARY KEY,
        dice1 SMALLINT,
        dice2 SMALLINT,
        dice3 SMALLINT,
        point SMALLINT,
        result_text TEXT,
        raw_result TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """)
    cur.close()
    conn.close()
    print("✅ Bảng sessions_new đã sẵn sàng", flush=True)

# ===== Parse helper =====
def parse_issue_id(issue_id):
    """Phân tích ID phiên thành các thành phần có thể sắp xếp"""
    try:
        parts = issue_id.split('-')
        if len(parts) == 3:
            game_name = parts[0]  # PK3_60S
            date_str = parts[1]   # 251004 (YYMMDD)
            session_num = parts[2] # 0569
            
            # Parse date để có thể so sánh
            year = 2000 + int(date_str[0:2])
            month = int(date_str[2:4])
            day = int(date_str[4:6])
            date_obj = datetime(year, month, day)
            
            # Parse session number
            session_int = int(session_num)
            
            return {
                'game_name': game_name,
                'date': date_obj,
                'date_str': date_str,
                'session_num': session_num,
                'session_int': session_int,
                'full_id': issue_id,
                'sort_key': (date_obj, session_int)
            }
    except Exception as e:
        print(f"❌ Lỗi parse issue_id {issue_id}: {e}")
    return None

def parse_result_string(r):
    if not r:
        return None
    r = str(r).strip()
    digits = [ch for ch in r if ch.isdigit()]
    if len(digits) >= 3:
        d1, d2, d3 = int(digits[0]), int(digits[1]), int(digits[2])
        return d1, d2, d3
    parts = [p for p in r.replace(',', ':').split(':') if p.strip().isdigit()]
    if len(parts) >= 3:
        return int(parts[0]), int(parts[1]), int(parts[2])
    return None

def point_to_text(point):
    return "TAI" if point >= 11 else "XIU"

# ===== LOGIN với RETRY =====
def get_token_with_retry():
    """Lấy token với retry mechanism"""
    for attempt in range(MAX_RETRIES):
        try:
            data = {
                "username": USERNAME,
                "password": PASSWORD,
                "siteKey": "6LfR_pYpAAAAAN20hVh1-AaBbVuf4oN7e4JU91dt",
                "captcha": ""
            }
            headers = {
                "accept": "application/json, text/plain, */*",
                "content-type": "application/x-www-form-urlencoded",
                "device-id": DEVICE_ID,
                "origin": "https://www.vn58q.bet",
                "referer": "https://www.vn58q.bet/login",
                "user-agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36",
            }
            resp = requests.post(LOGIN_URL, data=data, headers=headers, timeout=30)
            resp.raise_for_status()
            token = resp.json().get("access_token")
            if not token:
                raise Exception(f"Không lấy được access_token: {resp.text}")
            print(f"🔑 Lấy access_token thành công (lần {attempt + 1})", flush=True)
            return token
        except Exception as e:
            print(f"❌ Lỗi login lần {attempt + 1}/{MAX_RETRIES}: {e}", flush=True)
            if attempt < MAX_RETRIES - 1:
                print(f"⏳ Chờ {RETRY_INTERVAL} giây trước khi thử lại...", flush=True)
                time.sleep(RETRY_INTERVAL)
            else:
                print("🚫 Đã thử tối đa số lần, bỏ qua fetch lần này", flush=True)
    return None

# ===== Save rows =====
def save_rows(rows):
    if not rows:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    for r in rows:
        try:
            issue_id = r.get("issueId") or r.get("issue_id")
            raw_result = r.get("result")
            parsed = parse_result_string(raw_result)
            if not issue_id or not parsed:
                print(f"[{datetime.now()}] ⚠️ Bỏ qua row: {r}", flush=True)
                continue
            dice1, dice2, dice3 = parsed
            point = dice1 + dice2 + dice3
            result_text = point_to_text(point)
            cur.execute("""
                INSERT INTO sessions_new (issue_id, dice1, dice2, dice3, point, result_text, raw_result)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (issue_id) DO NOTHING
            """, (issue_id, dice1, dice2, dice3, point, result_text, str(raw_result)))
            inserted += 1
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Lỗi insert row: {e}", flush=True)
    conn.commit()
    cur.close()
    conn.close()
    return inserted

# ===== Fetch & save với RETRY =====
def fetch_and_save_with_retry():
    """Fetch và save dữ liệu với retry mechanism"""
    for attempt in range(MAX_RETRIES):
        try:
            print(f"🔄 Bắt đầu fetch dữ liệu (lần {attempt + 1}/{MAX_RETRIES})...", flush=True)
            
            token = get_token_with_retry()
            if not token:
                print("❌ Không lấy được token, thử lại...", flush=True)
                continue
                
            headers = {
                "accept": "application/json, text/plain, */*",
                "authorization": f"Bearer {token}",
                "device-id": DEVICE_ID,
            }
            
            resp = requests.get(API_URL, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            rows = data if isinstance(data, list) else data.get("list") or data.get("data") or []
            
            saved = save_rows(rows)
            print(f"[{datetime.now()}] ✅ Fetch thành công! Lưu {saved}/{len(rows)} phiên", flush=True)
            return saved
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi mạng/API lần {attempt + 1}/{MAX_RETRIES}: {e}", flush=True)
        except Exception as e:
            print(f"❌ Lỗi không xác định lần {attempt + 1}/{MAX_RETRIES}: {e}", flush=True)
        
        # Chờ trước khi thử lại (trừ lần cuối)
        if attempt < MAX_RETRIES - 1:
            print(f"⏳ Chờ {RETRY_INTERVAL} giây trước khi thử lại fetch...", flush=True)
            time.sleep(RETRY_INTERVAL)
        else:
            print("🚫 Đã thử fetch tối đa số lần, chuyển sang chu kỳ tiếp theo", flush=True)
    
    return 0

# ===== Statistics for Web =====
def get_continuous_chunks_vn58():
    """Lấy các chuỗi dữ liệu liên tục cho ID phức tạp"""
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT issue_id FROM sessions_new ORDER BY issue_id")
    all_issues = [row[0] for row in cur.fetchall()]
    
    if not all_issues:
        return []
    
    # Parse tất cả ID
    parsed_issues = []
    for issue_id in all_issues:
        parsed = parse_issue_id(issue_id)
        if parsed:
            parsed_issues.append(parsed)
    
    # Nhóm theo ngày
    daily_groups = {}
    for parsed in parsed_issues:
        date_key = parsed['date_str']
        if date_key not in daily_groups:
            daily_groups[date_key] = []
        daily_groups[date_key].append(parsed)
    
    # Tìm chuỗi liên tục trong từng ngày
    all_chunks = []
    for date_key, day_sessions in daily_groups.items():
        day_sessions.sort(key=lambda x: x['session_int'])
        
        current_chunk = [day_sessions[0]['full_id']]
        for i in range(1, len(day_sessions)):
            current_session = day_sessions[i]['session_int']
            prev_session = day_sessions[i-1]['session_int']
            
            if current_session == prev_session + 1:
                current_chunk.append(day_sessions[i]['full_id'])
            else:
                all_chunks.append(current_chunk)
                current_chunk = [day_sessions[i]['full_id']]
        
        all_chunks.append(current_chunk)
    
    cur.close()
    conn.close()
    return all_chunks

def get_statistics():
    """Lấy thống kê dữ liệu cho web"""
    conn = get_conn()
    if not conn:
        return None
    
    cur = conn.cursor()
    stats = {}
    
    try:
        # Tổng số phiên
        cur.execute("SELECT COUNT(*) FROM sessions_new")
        stats['total_sessions'] = cur.fetchone()[0]
        
        # Phiên đầu tiên và cuối cùng
        cur.execute("SELECT issue_id, dice1, dice2, dice3, point, result_text FROM sessions_new ORDER BY issue_id ASC LIMIT 1")
        stats['first_session'] = cur.fetchone()
        
        cur.execute("SELECT issue_id, dice1, dice2, dice3, point, result_text FROM sessions_new ORDER BY issue_id DESC LIMIT 1")
        stats['last_session'] = cur.fetchone()
        
        # Phiên trùng
        cur.execute("SELECT issue_id, COUNT(*) FROM sessions_new GROUP BY issue_id HAVING COUNT(*) > 1")
        stats['duplicate_sessions'] = cur.fetchall()
        
        # Dữ liệu mới nhất (20 phiên)
        cur.execute("SELECT issue_id, dice1, dice2, dice3, point, result_text FROM sessions_new ORDER BY issue_id DESC LIMIT 20")
        stats['recent_sessions'] = cur.fetchall()
        
        # Thống kê chuỗi liên tục
        chunks = get_continuous_chunks_vn58()
        stats['continuous_chunks'] = len(chunks)
        stats['chunks_info'] = []
        for i, chunk in enumerate(chunks[:10]):
            if chunk:
                first_parsed = parse_issue_id(chunk[0])
                last_parsed = parse_issue_id(chunk[-1])
                if first_parsed and last_parsed:
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

# ===== XUẤT FILE TOÀN BỘ =====
def export_full_txt():
    """Xuất file TXT toàn bộ"""
    conn = get_conn()
    if not conn:
        return "❌ Lỗi kết nối database", 500
    
    cur = conn.cursor()
    try:
        cur.execute("SELECT issue_id, dice1, dice2, dice3, point, result_text FROM sessions_new ORDER BY issue_id ASC")
        rows = cur.fetchall()
        
        if not rows:
            return "❌ Không có dữ liệu để xuất", 404
        
        filename = f"vn58_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
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
        cur.execute("SELECT issue_id, dice1, dice2, dice3, point, result_text FROM sessions_new ORDER BY issue_id ASC")
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
        
        filename = f"vn58_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
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

# ===== XUẤT FILE THEO CHUỖI LIÊN TỤC =====
def export_continuous_chunks_txt():
    """Xuất nhiều file TXT theo chuỗi liên tục - đặt tên data1, data2..."""
    conn = get_conn()
    if not conn:
        return "❌ Lỗi kết nối database", 500
    
    try:
        # Lấy các chuỗi liên tục
        chunks = get_continuous_chunks_vn58()
        
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
                        SELECT issue_id, dice1, dice2, dice3, point, result_text 
                        FROM sessions_new 
                        WHERE issue_id IN ({placeholders}) 
                        ORDER BY issue_id
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
            headers={"Content-Disposition": "attachment;filename=vn58_data_continuous.zip"}
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
        chunks = get_continuous_chunks_vn58()
        
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
                        SELECT issue_id, dice1, dice2, dice3, point, result_text 
                        FROM sessions_new 
                        WHERE issue_id IN ({placeholders}) 
                        ORDER BY issue_id
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
            headers={"Content-Disposition": "attachment;filename=vn58_data_continuous.zip"}
        )
        
    except Exception as e:
        return f"❌ Lỗi khi xuất file: {e}", 500
    finally:
        conn.close()

# ===== FLASK WEB APP =====
app = Flask(__name__)

# HTML Template
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎲 Quản Lý Dữ Liệu VN58</title>
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
            <h1>🎲 Quản Lý Dữ Liệu VN58</h1>
            <p>Theo dõi và phân tích dữ liệu PK3_60S</p>
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
                        {% if 'TAI' in session[5] %}badge-success
                        {% else %}badge-danger{% endif %}">
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
    """Xuất file TXT toàn bộ"""
    return export_full_txt()

@app.route("/export/json")
def export_json():
    """Xuất file JSON toàn bộ"""
    return export_full_json()

@app.route("/export/continuous-txt")
def export_continuous_txt():
    """Xuất nhiều file TXT theo chuỗi liên tục"""
    return export_continuous_chunks_txt()

@app.route("/export/continuous-json")
def export_continuous_json():
    """Xuất nhiều file JSON theo chuỗi liên tục"""
    return export_continuous_chunks_json()

@app.route("/api/data")
def api_data():
    """API trả về JSON data"""
    stats = get_statistics()
    return jsonify(stats)

# ===== Loop task với RETRY =====
def loop_task():
    init_db()
    while True:
        try:
            saved_count = fetch_and_save_with_retry()
            if saved_count > 0:
                print(f"✅ Đã thu thập thành công {saved_count} phiên mới", flush=True)
            else:
                print("⚠️ Không có phiên mới nào được thu thập", flush=True)
        except Exception as e:
            print(f"❌ Lỗi nghiêm trọng trong loop_task: {e}", flush=True)
        
        print(f"⏳ Chờ {INTERVAL} giây để fetch lần tiếp theo...\n", flush=True)
        time.sleep(INTERVAL)

if __name__ == "__main__":
    t = threading.Thread(target=loop_task, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
