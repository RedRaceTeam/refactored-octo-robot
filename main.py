import os
import json
import asyncio
import logging
import uvicorn 
import sqlite3
from datetime import datetime
from typing import List, Dict, Any
from contextlib import asynccontextmanager
from hashlib import sha256

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from google import genai
from google.genai import types

# ===== CONFIG =====
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    logger.warning("GEMINI_API_KEY not found in environment")

client = genai.Client(api_key=GEMINI_API_KEY)

# ===== DATABASE =====
DB_PATH = "liberty.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Users table
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            role TEXT DEFAULT 'user',
            status TEXT DEFAULT 'active',
            last_activity TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Sources table
    c.execute('''
        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            active INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Check if admin exists
    admin_hash = sha256("BWOAH 2026".encode()).hexdigest()
    c.execute('SELECT username FROM users WHERE username = ?', ('kimi',))
    if not c.fetchone():
        c.execute('''
            INSERT INTO users (username, password_hash, role, status, last_activity)
            VALUES (?, ?, ?, ?, ?)
        ''', ('kimi', admin_hash, 'admin', 'active', datetime.now().isoformat()))
    
    # Add default source if none
    c.execute('SELECT id FROM sources LIMIT 1')
    if not c.fetchone():
        c.execute('''
            INSERT INTO sources (name, url, active) VALUES (?, ?, ?)
        ''', ('Test Stream', 'https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8', 1))
    
    conn.commit()
    conn.close()

def get_db():
    return sqlite3.connect(DB_PATH)

init_db()

# ===== OPENF1 API =====
OPENF1_BASE = "https://api.openf1.org/v1"

async def fetch_openf1(endpoint: str, params: Dict[str, Any] = None) -> List[Dict]:
    try:
        async with httpx.AsyncClient(timeout=10.0) as http_client:
            url = f"{OPENF1_BASE}/{endpoint}"
            response = await http_client.get(url, params=params)
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logger.error(f"OpenF1 error: {e}")
        return []

async def get_current_session() -> Dict:
    data = await fetch_openf1("sessions", {"year": 2026})
    if not data:
        return {}
    sessions = sorted(data, key=lambda x: x.get("date_start", ""), reverse=True)
    return sessions[0] if sessions else {}

async def get_live_timing(session_key: str) -> Dict:
    positions = await fetch_openf1("position", {"session_key": session_key})
    intervals = await fetch_openf1("intervals", {"session_key": session_key})
    drivers = await fetch_openf1("drivers", {"session_key": session_key})
    return {"positions": positions, "intervals": intervals, "drivers": drivers}

# ===== WEBSOCKET MANAGER =====
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Broadcast error: {e}")

manager = ConnectionManager()

# ===== LIFESPAN =====
telemetry_running = True
last_telemetry_data = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(update_telemetry_background())
    logger.info("Liberty Formula started")
    yield
    telemetry_running = False
    task.cancel()
    logger.info("Liberty Formula shutdown")

# ===== APP =====
app = FastAPI(title="Liberty Formula API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== BACKGROUND TASK =====
async def update_telemetry_background():
    global last_telemetry_data
    while telemetry_running:
        try:
            session = await get_current_session()
            if not session:
                await asyncio.sleep(5)
                continue

            session_key = session.get("session_key")
            if not session_key:
                await asyncio.sleep(5)
                continue

            timing = await get_live_timing(str(session_key))
            if timing:
                message = {
                    "type": "telemetry",
                    "data": timing,
                    "session": {
                        "name": session.get("meeting_name", "Unknown GP"),
                        "country": session.get("country_name", ""),
                        "flag": session.get("country_flag", "🏁"),
                        "year": session.get("year", "2026")
                    },
                    "timestamp": datetime.now().isoformat()
                }
                last_telemetry_data = message
                await manager.broadcast(json.dumps(message))

        except Exception as e:
            logger.error(f"Telemetry error: {e}")

        await asyncio.sleep(3)

# ===== USERS API =====
@app.get("/api/users")
async def get_users():
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT username, role, status, last_activity FROM users ORDER BY created_at')
    rows = c.fetchall()
    conn.close()
    
    users = [{
        "username": row[0],
        "role": row[1],
        "status": row[2],
        "lastActivity": row[3]
    } for row in rows]
    
    return {"status": "ok", "users": users}

@app.post("/api/users")
async def add_user(request: Request):
    data = await request.json()
    username = data.get("username")
    password = data.get("password")
    role = data.get("role", "user")
    
    if not username or not password:
        return JSONResponse({"status": "error", "message": "Username and password required"}, 400)
    
    conn = get_db()
    c = conn.cursor()
    
    c.execute('SELECT username FROM users WHERE username = ?', (username,))
    if c.fetchone():
        conn.close()
        return JSONResponse({"status": "error", "message": "User already exists"}, 400)
    
    password_hash = sha256(password.encode()).hexdigest()
    c.execute('''
        INSERT INTO users (username, password_hash, role, status, last_activity)
        VALUES (?, ?, ?, ?, ?)
    ''', (username, password_hash, role, 'active', datetime.now().isoformat()))
    
    conn.commit()
    conn.close()
    
    return {"status": "ok", "message": "User added"}

@app.delete("/api/users/{username}")
async def delete_user(username: str):
    if username == "kimi":
        return JSONResponse({"status": "error", "message": "Cannot delete admin"}, 400)
    
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM users WHERE username = ?', (username,))
    conn.commit()
    conn.close()
    
    return {"status": "ok", "message": "User deleted"}

# ===== SOURCES API =====
@app.get("/api/sources")
async def get_sources():
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id, name, url, active FROM sources ORDER BY created_at')
    rows = c.fetchall()
    conn.close()
    
    sources = [{
        "id": row[0],
        "name": row[1],
        "url": row[2],
        "active": bool(row[3])
    } for row in rows]
    
    return {"status": "ok", "sources": sources}

@app.post("/api/sources")
async def add_source(request: Request):
    data = await request.json()
    name = data.get("name")
    url = data.get("url")
    
    if not name or not url:
        return JSONResponse({"status": "error", "message": "Name and URL required"}, 400)
    
    conn = get_db()
    c = conn.cursor()
    c.execute('INSERT INTO sources (name, url, active) VALUES (?, ?, ?)', (name, url, 0))
    conn.commit()
    conn.close()
    
    return {"status": "ok", "message": "Source added"}

@app.delete("/api/sources/{source_id}")
async def delete_source(source_id: int):
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM sources WHERE id = ?', (source_id,))
    conn.commit()
    conn.close()
    
    return {"status": "ok", "message": "Source deleted"}

@app.put("/api/sources/{source_id}/active")
async def set_active_source(source_id: int):
    conn = get_db()
    c = conn.cursor()
    c.execute('UPDATE sources SET active = 0')
    c.execute('UPDATE sources SET active = 1 WHERE id = ?', (source_id,))
    conn.commit()
    conn.close()
    
    return {"status": "ok", "message": "Source activated"}

# ===== AUTH API =====
@app.post("/api/login")
async def login(request: Request):
    data = await request.json()
    username = data.get("username")
    password = data.get("password")
    
    if not username or not password:
        return {"status": "error", "message": "Username and password required"}
    
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT username, password_hash, role, status, last_activity FROM users WHERE username = ?', (username,))
    row = c.fetchone()
    conn.close()
    
    if not row:
        return {"status": "error", "message": "Invalid credentials"}
    
    password_hash = sha256(password.encode()).hexdigest()
    if password_hash != row[1]:
        return {"status": "error", "message": "Invalid credentials"}
    
    # Update last activity
    conn = get_db()
    c = conn.cursor()
    c.execute('UPDATE users SET last_activity = ? WHERE username = ?', (datetime.now().isoformat(), username))
    conn.commit()
    conn.close()
    
    return {
        "status": "ok",
        "user": {
            "username": row[0],
            "role": row[2],
            "status": row[3],
            "lastActivity": row[4]
        }
    }

@app.get("/api/session")
async def get_session():
    session = last_telemetry_data.get("session", {})
    return {"status": "ok", "session": session}

# ===== TELEMETRY API =====
@app.get("/api/telemetry")
async def get_telemetry():
    if not last_telemetry_data:
        return {"status": "error", "message": "No telemetry data"}
    return {"status": "ok", "data": last_telemetry_data}

# ===== AI API =====
@app.get("/api/ai/comment")
async def get_ai_comment():
    if not GEMINI_API_KEY:
        return {"status": "error", "message": "Gemini not configured"}

    if not last_telemetry_data:
        return {"status": "error", "message": "No telemetry data"}

    try:
        telemetry = last_telemetry_data.get("data", {})
        positions = telemetry.get("positions", [])

        if not positions:
            return {"status": "error", "message": "No position data"}

        top_positions = sorted(positions, key=lambda x: x.get("position", 999))[:3]
        top_text = ", ".join([f"#{p.get('driver_number')} (P{p.get('position')})" for p in top_positions])

        prompt = f"""
        You are a professional F1 commentator.
        Current top 3: {top_text}.
        Provide a short, exciting commentary (1-2 sentences) about the race situation.
        Focus on the battle for the lead.
        """

        system = "You are a professional F1 commentator. Speak with energy and excitement."

        response = client.models.generate_content(
            model='gemini-3.6-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=system
            )
        )

        if response.text:
            return {"status": "ok", "commentary": response.text}
        return {"status": "error", "message": "No commentary generated"}

    except Exception as e:
        logger.error(f"AI error: {e}")
        return {"status": "error", "message": str(e)}

# ===== EXPORT API =====
@app.get("/api/export")
async def export_data():
    conn = get_db()
    c = conn.cursor()
    
    c.execute('SELECT username, role, status, last_activity FROM users')
    users = [{"username": row[0], "role": row[1], "status": row[2], "lastActivity": row[3]} for row in c.fetchall()]
    
    c.execute('SELECT id, name, url, active FROM sources')
    sources = [{"id": row[0], "name": row[1], "url": row[2], "active": bool(row[3])} for row in c.fetchall()]
    
    conn.close()
    
    return {
        "status": "ok",
        "data": {
            "exported": datetime.now().isoformat(),
            "users": users,
            "sources": sources
        }
    }

# ===== WEBSOCKET =====
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)

# ===== ROOT =====
@app.get("/")
async def root():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

# ===== RUN =====
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
