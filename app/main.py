import asyncio
from typing import Optional
import ipaddress
import re
import time as _time_module
from collections import defaultdict
from urllib.parse import urlparse
from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from contextlib import asynccontextmanager
import httpx
import os
from dotenv import load_dotenv
load_dotenv()
from app.scraper import scrape_carrier, fetch_safety_data, fetch_insurance_data, close_clients
from app.task_manager import task_manager
from app.fmcsa_register import scrape_fmcsa_register
from app.broker_snapshot import scrape_broker_snapshot
from app.auth import create_token, verify_token, require_auth
from app.database import (
    connect_db, close_db,
    upsert_carrier, fetch_carriers, delete_carrier as db_delete_carrier,
    get_carrier_count, get_carrier_dashboard_stats,
    update_carrier_insurance as db_update_carrier_insurance,
    update_carrier_safety as db_update_carrier_safety, get_carriers_by_mc_range,
    fetch_users, fetch_user_by_email, create_user, update_user, delete_user,
    get_user_password_hash,
    fetch_blocked_ips, block_ip, unblock_ip, is_ip_blocked,
    save_fmcsa_register_entries, fetch_fmcsa_register_by_date,
    get_fmcsa_extracted_dates, get_fmcsa_categories, delete_fmcsa_entries_before_date,
    save_new_venture_entries, fetch_new_ventures, fetch_new_venture_by_id,
    get_new_venture_count, get_new_venture_scraped_dates, delete_new_venture,
    fetch_insurance_history, fetch_inspections,
)
@asynccontextmanager
async def lifespan(application: FastAPI):
    await connect_db()
    yield
    await close_clients()
    await close_db()
app = FastAPI(lifespan=lifespan)
_PUBLIC_PATHS: set[str] = {
    "/health",
    "/healthz",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/api/get-ip",
    "/api/auth/login",
    "/api/auth/register",
    "/api/blocked-ips/check",
    "/api/carriers",
    "/api/carriers/count",
    "/api/carriers/dashboard-stats",
}
_PUBLIC_PREFIXES: tuple[str, ...] = (
    "/docs",
    "/redoc",
    "/openapi.json",
    "/api/blocked-ips/check/",
    "/api/carriers/",
)
def _get_request_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.headers.get("x-real-ip", "") or (request.client.host if request.client else "")


# ---------------------------------------------------------------------------
# Rate limiter (in-memory, per-IP)
# ---------------------------------------------------------------------------
_rate_limit_store: dict[str, list[float]] = defaultdict(list)
_RATE_LIMIT_WINDOW = 60  # seconds
_RATE_LIMIT_MAX_REQUESTS = 15  # max attempts per window for auth endpoints

def _is_rate_limited(key: str, max_requests: int = _RATE_LIMIT_MAX_REQUESTS) -> bool:
    now = _time_module.time()
    window_start = now - _RATE_LIMIT_WINDOW
    hits = _rate_limit_store[key]
    # prune old entries
    _rate_limit_store[key] = [t for t in hits if t > window_start]
    if len(_rate_limit_store[key]) >= max_requests:
        return True
    _rate_limit_store[key].append(now)
    return False


def _require_admin(request: Request) -> Optional[dict]:
    user = getattr(request.state, "user", None)
    if not user or user.get("role") != "admin":
        return None
    return user


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if request.method == "OPTIONS":
            return await call_next(request)
        if path in _PUBLIC_PATHS or any(path.startswith(p) for p in _PUBLIC_PREFIXES):
            return await call_next(request)
        user_payload = await require_auth(request)
        if user_payload is None:
            return JSONResponse(
                status_code=401,
                content={"error": "Authentication required. Please log in."},
            )
        request.state.user = user_payload
        return await call_next(request)


class IPBlockMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if request.method == "OPTIONS" or path in ("/health", "/healthz"):
            return await call_next(request)
        client_ip = _get_request_ip(request)
        if client_ip:
            blocked = await is_ip_blocked(client_ip)
            if blocked:
                return JSONResponse(
                    status_code=403,
                    content={"error": "Your IP address has been blocked."},
                )
        return await call_next(request)


app.add_middleware(AuthMiddleware)
app.add_middleware(IPBlockMiddleware)
app.add_middleware(GZipMiddleware, minimum_size=500)
_cors_raw = os.getenv("CORS_ORIGINS", "")
if not _cors_raw:
    import warnings
    warnings.warn("CORS_ORIGINS is not set. Defaulting to restrictive CORS policy.")
    _cors_origins = []
else:
    _cors_origins = [o.strip() for o in _cors_raw.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins if _cors_origins else ["*"],
    allow_credentials=bool(_cors_origins),
    allow_methods=["*"],
    allow_headers=["*"],
)
ALLOWED_DOMAINS = {
    "safer.fmcsa.dot.gov",
    "ai.fmcsa.dot.gov",
    "searchcarriers.com",
    "www.searchcarriers.com",
    "li-public.fmcsa.dot.gov",
}
@app.get("/healthz")
async def healthz():
    try:
        from app.database import get_pool
        pool = get_pool()
        await pool.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception:
        return JSONResponse(status_code=503, content={"status": "unhealthy", "error": "database unreachable"})
@app.get("/health")
async def health():
    return {"status": "ok", "message": "FMCSA Scraper Backend is running"}
@app.get("/api/get-ip")
async def get_ip(request: Request):
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        ip = forwarded.split(",")[0].strip()
    else:
        ip = request.headers.get("x-real-ip", "") or (request.client.host if request.client else "")
    return {"ip": ip}
@app.get("/api/proxy")
async def proxy(url: str = Query(...)):
    parsed = urlparse(url)
    if not parsed.hostname or parsed.hostname not in ALLOWED_DOMAINS:
        return JSONResponse(status_code=403, content={"error": "Domain not allowed"})
    try:
        if "searchcarriers.com" in url:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.5",
                "Referer": "https://searchcarriers.com/",
                "Origin": "https://searchcarriers.com",
            }
        else:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
            resp = await client.get(url, headers=headers)
        if resp.is_redirect:
            return JSONResponse(status_code=502, content={"error": "Upstream redirect not allowed"})
        content_type = resp.headers.get("content-type", "text/html; charset=utf-8")
        return PlainTextResponse(content=resp.text, status_code=resp.status_code, headers={
            "Content-Type": content_type,
            "Access-Control-Allow-Origin": "*",
        })
    except Exception:
        return JSONResponse(status_code=500, content={"error": "Proxy request failed"})
@app.post("/api/fmcsa-register")
async def fmcsa_register(request: Request):
    body = await request.json()
    date_str = body.get("date", None)
    save_to_db = body.get("saveToDb", False)
    result = await scrape_fmcsa_register(date_str)
    if result.get("success") and save_to_db:
        entries = result.get("entries", [])
        extracted_date = result.get("date", "")
        db_result = await save_fmcsa_register_entries(entries, extracted_date)
        result["dbSaved"] = db_result.get("saved", 0)
        result["dbSkipped"] = db_result.get("skipped", 0)
    if result.get("success"):
        return result
    return JSONResponse(status_code=500 if "error" in result else 400, content=result)
@app.get("/api/fmcsa-register/entries")
async def get_fmcsa_entries(
    extracted_date: str = Query(...),
    category: str = Query(None),
    search: str = Query(None),
):
    entries = await fetch_fmcsa_register_by_date(extracted_date, category, search)
    return {"success": True, "count": len(entries), "entries": entries}
@app.get("/api/fmcsa-register/dates")
async def get_fmcsa_dates():
    dates = await get_fmcsa_extracted_dates()
    return {"success": True, "dates": dates}
@app.get("/api/scrape/carrier/{mc_number}")
async def scrape_single_carrier(mc_number: str):
    data = await scrape_carrier(mc_number)
    if data:
        return data
    return JSONResponse(status_code=404, content={"error": "No data found"})
@app.get("/api/scrape/safety/{dot_number}")
async def scrape_safety(dot_number: str):
    data = await fetch_safety_data(dot_number)
    return data
@app.get("/api/scrape/insurance/{dot_number}")
async def scrape_insurance(dot_number: str):
    data = await fetch_insurance_data(dot_number)
    return data
@app.post("/api/tasks/scraper/start")
async def start_scraper_task(request: Request):
    body = await request.json()
    config = body.get("config", {})
    task_id = await task_manager.start_scraper_task(config)
    return {"task_id": task_id, "status": "started"}
@app.post("/api/tasks/scraper/stop")
async def stop_scraper_task(request: Request):
    body = await request.json()
    task_id = body.get("task_id")
    if not task_id:
        return JSONResponse(status_code=400, content={"error": "task_id required"})
    task_manager.stop_task(task_id)
    return {"task_id": task_id, "status": "stopping"}
@app.get("/api/tasks/scraper/status")
async def get_scraper_status(task_id: str = Query(...)):
    status = task_manager.get_task_status(task_id)
    if not status:
        return JSONResponse(status_code=404, content={"error": "Task not found"})
    return status
@app.post("/api/tasks/insurance/start")
async def start_insurance_task(request: Request):
    body = await request.json()
    config = body.get("config", {})
    task_id = await task_manager.start_insurance_task(config)
    return {"task_id": task_id, "status": "started"}
@app.post("/api/tasks/insurance/stop")
async def stop_insurance_task(request: Request):
    body = await request.json()
    task_id = body.get("task_id")
    if not task_id:
        return JSONResponse(status_code=400, content={"error": "task_id required"})
    task_manager.stop_task(task_id)
    return {"task_id": task_id, "status": "stopping"}
@app.get("/api/tasks/insurance/status")
async def get_insurance_status(task_id: str = Query(...)):
    status = task_manager.get_task_status(task_id)
    if not status:
        return JSONResponse(status_code=404, content={"error": "Task not found"})
    return status
@app.get("/api/tasks/scraper/data")
async def get_scraper_data(task_id: str = Query(...)):
    data = task_manager.get_task_data(task_id)
    if data is None:
        return JSONResponse(status_code=404, content={"error": "Task not found"})
    return data
@app.get("/api/tasks/active")
async def get_active_task(task_type: str = Query("scraper")):
    task_id = task_manager.get_active_task_id(task_type)
    if not task_id:
        return {"task_id": None}
    status = task_manager.get_task_status(task_id)
    return {"task_id": task_id, "task": status}
@app.get("/api/tasks")
async def list_tasks():
    return task_manager.list_tasks()
@app.get("/api/carriers")
async def api_fetch_carriers(
    mc_number: str = Query(None),
    dot_number: str = Query(None),
    legal_name: str = Query(None),
    entity_type: str = Query(None),
    active: str = Query(None),
    state: str = Query(None),
    has_email: str = Query(None),
    has_boc3: str = Query(None),
    has_company_rep: str = Query(None),
    classification: str = Query(None),
    carrier_operation: str = Query(None),
    cargo: str = Query(None),
    hazmat: str = Query(None),
    power_units_min: str = Query(None),
    power_units_max: str = Query(None),
    drivers_min: str = Query(None),
    drivers_max: str = Query(None),
    insurance_required: str = Query(None),
    bipd_min: str = Query(None),
    bipd_max: str = Query(None),
    ins_effective_date_from: str = Query(None),
    ins_effective_date_to: str = Query(None),
    bipd_on_file: str = Query(None),
    cargo_on_file: str = Query(None),
    bond_on_file: str = Query(None),
    trust_fund_on_file: str = Query(None),
    ins_cancellation_date_from: str = Query(None),
    ins_cancellation_date_to: str = Query(None),
    years_in_business_min: str = Query(None),
    years_in_business_max: str = Query(None),
    oos_min: str = Query(None),
    oos_max: str = Query(None),
    crashes_min: str = Query(None),
    crashes_max: str = Query(None),
    injuries_min: str = Query(None),
    injuries_max: str = Query(None),
    fatalities_min: str = Query(None),
    fatalities_max: str = Query(None),
    toway_min: str = Query(None),
    toway_max: str = Query(None),
    inspections_min: str = Query(None),
    inspections_max: str = Query(None),
    insurance_company: str = Query(None),
    renewal_policy_months: str = Query(None),
    renewal_date_from: str = Query(None),
    renewal_date_to: str = Query(None),
    limit: int = Query(None),
    offset: int = Query(0),
):
    filters = {}
    if mc_number: filters["mc_number"] = mc_number
    if dot_number: filters["dot_number"] = dot_number
    if legal_name: filters["legal_name"] = legal_name
    if entity_type: filters["entity_type"] = entity_type
    if active: filters["active"] = active
    if state: filters["state"] = state
    if has_email: filters["has_email"] = has_email
    if has_boc3: filters["has_boc3"] = has_boc3
    if has_company_rep: filters["has_company_rep"] = has_company_rep
    if classification: filters["classification"] = classification.split(",")
    if carrier_operation: filters["carrier_operation"] = carrier_operation.split(",")
    if cargo: filters["cargo"] = cargo.split(",")
    if hazmat: filters["hazmat"] = hazmat
    if power_units_min: filters["power_units_min"] = power_units_min
    if power_units_max: filters["power_units_max"] = power_units_max
    if drivers_min: filters["drivers_min"] = drivers_min
    if drivers_max: filters["drivers_max"] = drivers_max
    if insurance_required: filters["insurance_required"] = insurance_required.split(",")
    if bipd_min: filters["bipd_min"] = bipd_min
    if bipd_max: filters["bipd_max"] = bipd_max
    if ins_effective_date_from: filters["ins_effective_date_from"] = ins_effective_date_from
    if ins_effective_date_to: filters["ins_effective_date_to"] = ins_effective_date_to
    if bipd_on_file: filters["bipd_on_file"] = bipd_on_file
    if cargo_on_file: filters["cargo_on_file"] = cargo_on_file
    if bond_on_file: filters["bond_on_file"] = bond_on_file
    if trust_fund_on_file: filters["trust_fund_on_file"] = trust_fund_on_file
    if ins_cancellation_date_from: filters["ins_cancellation_date_from"] = ins_cancellation_date_from
    if ins_cancellation_date_to: filters["ins_cancellation_date_to"] = ins_cancellation_date_to
    if years_in_business_min: filters["years_in_business_min"] = years_in_business_min
    if years_in_business_max: filters["years_in_business_max"] = years_in_business_max
    if oos_min: filters["oos_min"] = oos_min
    if oos_max: filters["oos_max"] = oos_max
    if crashes_min: filters["crashes_min"] = crashes_min
    if crashes_max: filters["crashes_max"] = crashes_max
    if injuries_min: filters["injuries_min"] = injuries_min
    if injuries_max: filters["injuries_max"] = injuries_max
    if fatalities_min: filters["fatalities_min"] = fatalities_min
    if fatalities_max: filters["fatalities_max"] = fatalities_max
    if toway_min: filters["toway_min"] = toway_min
    if toway_max: filters["toway_max"] = toway_max
    if inspections_min: filters["inspections_min"] = inspections_min
    if inspections_max: filters["inspections_max"] = inspections_max
    if insurance_company: filters["insurance_company"] = insurance_company
    if renewal_policy_months: filters["renewal_policy_months"] = renewal_policy_months
    if renewal_date_from: filters["renewal_date_from"] = renewal_date_from
    if renewal_date_to: filters["renewal_date_to"] = renewal_date_to
    if offset > 0: filters["offset"] = offset
    if limit is not None:
        filters["limit"] = limit
    else:
        has_filters = any(v for k, v in filters.items() if k not in ("limit", "offset"))
        if not has_filters:
            filters["limit"] = 200
    result = await fetch_carriers(filters)
    return result
@app.post("/api/carriers")
async def api_upsert_carrier(request: Request):
    body = await request.json()
    ok = await upsert_carrier(body)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=400, content={"success": False, "error": "Failed to upsert carrier"})
@app.post("/api/carriers/batch")
async def api_upsert_carriers_batch(request: Request):
    body = await request.json()
    carriers_list = body.get("carriers", [])
    saved = 0
    failed = 0
    batch_size = 10
    for i in range(0, len(carriers_list), batch_size):
        batch = carriers_list[i:i + batch_size]
        results = await asyncio.gather(
            *(upsert_carrier(c) for c in batch),
            return_exceptions=True,
        )
        for ok in results:
            if ok is True:
                saved += 1
            else:
                failed += 1
    return {"success": failed == 0, "saved": saved, "failed": failed}
@app.delete("/api/carriers/{mc_number}")
async def api_delete_carrier(mc_number: str, request: Request):
    if not _require_admin(request):
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    ok = await db_delete_carrier(mc_number)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "Carrier not found"})
@app.get("/api/carriers/count")
async def api_get_carrier_count():
    count = await get_carrier_count()
    return {"count": count}
@app.get("/api/carriers/dashboard-stats")
async def api_get_carrier_dashboard_stats():
    stats = await get_carrier_dashboard_stats()
    return stats
@app.put("/api/carriers/{dot_number}/insurance")
async def api_update_carrier_insurance(dot_number: str, request: Request):
    body = await request.json()
    policies = body.get("policies", [])
    ok = await db_update_carrier_insurance(dot_number, policies)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "Carrier not found or update failed"})
@app.put("/api/carriers/{dot_number}/safety")
async def api_update_carrier_safety(dot_number: str, request: Request):
    body = await request.json()
    ok = await db_update_carrier_safety(dot_number, body)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "Carrier not found or update failed"})
@app.get("/api/carriers/range")
async def api_get_carriers_by_range(
    start: str = Query(...),
    end: str = Query(...),
):
    data = await get_carriers_by_mc_range(start, end)
    return data
@app.post("/api/auth/login")
async def api_auth_login(request: Request):
    client_ip = _get_request_ip(request)
    if _is_rate_limited(f"login:{client_ip}"):
        return JSONResponse(status_code=429, content={"error": "Too many login attempts. Please try again later."})
    body = await request.json()
    email = body.get("email", "").strip().lower()
    password = body.get("password", "")
    if not email or not password:
        return JSONResponse(status_code=400, content={"error": "Email and password are required"})
    stored_hash = await get_user_password_hash(email)
    if not stored_hash:
        return JSONResponse(status_code=401, content={"error": "Invalid email or password"})
    import bcrypt
    if stored_hash.startswith("$2b$") or stored_hash.startswith("$2a$"):
        if not bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8")):
            return JSONResponse(status_code=401, content={"error": "Invalid email or password"})
    else:
        return JSONResponse(status_code=401, content={"error": "Invalid email or password"})
    user = await fetch_user_by_email(email)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Invalid email or password"})
    token = create_token(
        user_id=user["user_id"],
        email=user["email"],
        role=user["role"],
    )
    return {
        "token": token,
        "user": {
            "user_id": user["user_id"],
            "name": user["name"],
            "email": user["email"],
            "role": user["role"],
            "plan": user["plan"],
            "daily_limit": user["daily_limit"],
            "records_extracted_today": user["records_extracted_today"],
            "last_active": user.get("last_active", "Never"),
            "ip_address": user.get("ip_address", ""),
            "is_online": user.get("is_online", False),
            "is_blocked": user.get("is_blocked", False),
        },
    }
@app.post("/api/auth/register")
async def api_auth_register(request: Request):
    client_ip = _get_request_ip(request)
    if _is_rate_limited(f"register:{client_ip}", max_requests=5):
        return JSONResponse(status_code=429, content={"error": "Too many registration attempts. Please try again later."})
    body = await request.json()
    email = body.get("email", "").strip().lower()
    password = body.get("password", "")
    name = body.get("name", "")
    if not email or not password or not name:
        return JSONResponse(status_code=400, content={"error": "Name, email, and password are required"})
    if len(password) < 8:
        return JSONResponse(status_code=400, content={"error": "Password must be at least 8 characters long"})
    if not re.search(r"\d", password) or not re.search(r"[a-zA-Z]", password):
        return JSONResponse(status_code=400, content={"error": "Password must contain at least one letter and one number"})
    existing = await fetch_user_by_email(email)
    if existing:
        return JSONResponse(status_code=409, content={"error": "User with this email already exists"})
    import bcrypt
    import time as _time
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    user_data = {
        "user_id": body.get("user_id", f"user-{int(_time.time() * 1000)}"),
        "name": name,
        "email": email,
        "password_hash": password_hash,
        "role": "user",
        "plan": "Free",
        "daily_limit": 50,
        "records_extracted_today": 0,
        "last_active": "Now",
        "ip_address": body.get("ip_address", ""),
        "is_online": True,
        "is_blocked": False,
    }
    user = await create_user(user_data)
    if not user:
        return JSONResponse(status_code=500, content={"error": "Failed to create user"})
    token = create_token(
        user_id=user["user_id"],
        email=user["email"],
        role=user["role"],
    )
    user.pop("password_hash", None)
    return {"token": token, "user": user}
@app.get("/api/users")
async def api_fetch_users(request: Request):
    if not _require_admin(request):
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    users = await fetch_users()
    return users
@app.get("/api/users/by-email/{email:path}")
async def api_fetch_user_by_email(email: str, request: Request):
    if not _require_admin(request):
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    user = await fetch_user_by_email(email)
    if user:
        return user
    return JSONResponse(status_code=404, content={"error": "User not found"})
@app.post("/api/users")
async def api_create_user(request: Request):
    if not _require_admin(request):
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    body = await request.json()
    if body.get("password"):
        import bcrypt
        body["password_hash"] = bcrypt.hashpw(
            body.pop("password").encode("utf-8"),
            bcrypt.gensalt(),
        ).decode("utf-8")
    user = await create_user(body)
    if user:
        user.pop("password_hash", None)
        return user
    return JSONResponse(status_code=400, content={"error": "Failed to create user"})
@app.put("/api/users/{user_id}")
async def api_update_user(user_id: str, request: Request):
    current_user = getattr(request.state, "user", None)
    if not current_user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    body = await request.json()
    is_admin = current_user.get("role") == "admin"
    is_self = current_user.get("sub") == user_id
    if not is_admin and not is_self:
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    if is_self and not is_admin:
        _SELF_ALLOWED_FIELDS = {"name", "is_online", "last_active", "ip_address"}
        restricted = set(body.keys()) - _SELF_ALLOWED_FIELDS
        if restricted:
            return JSONResponse(
                status_code=403,
                content={"error": f"Cannot modify restricted fields: {', '.join(sorted(restricted))}"},
            )
    ok = await update_user(user_id, body)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "User not found"})
@app.delete("/api/users/{user_id}")
async def api_delete_user(user_id: str, request: Request):
    if not _require_admin(request):
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    ok = await delete_user(user_id)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "User not found"})
@app.post("/api/users/verify-password")
async def api_verify_password(request: Request):
    client_ip = _get_request_ip(request)
    if _is_rate_limited(f"verify:{client_ip}"):
        return JSONResponse(status_code=429, content={"error": "Too many attempts. Please try again later."})
    body = await request.json()
    email = body.get("email", "")
    password = body.get("password", "")
    stored_hash = await get_user_password_hash(email)
    if not stored_hash:
        return {"valid": False}
    import bcrypt
    if stored_hash.startswith("$2b$") or stored_hash.startswith("$2a$"):
        if password:
            is_valid = bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8"))
            return {"valid": is_valid}
        return {"valid": False}
    return {"valid": False}
@app.get("/api/blocked-ips")
async def api_fetch_blocked_ips(request: Request):
    if not _require_admin(request):
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    ips = await fetch_blocked_ips()
    return ips
@app.post("/api/blocked-ips")
async def api_block_ip(request: Request):
    if not _require_admin(request):
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    body = await request.json()
    ip = body.get("ip_address", "")
    try:
        ipaddress.ip_address(ip)
    except ValueError:
        return JSONResponse(status_code=400, content={"success": False, "error": "Invalid IP address format"})
    reason = body.get("reason", "No reason provided")
    ok = await block_ip(ip, reason)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=400, content={"success": False, "error": "Failed to block IP"})
@app.delete("/api/blocked-ips/{ip_address}")
async def api_unblock_ip(ip_address: str, request: Request):
    if not _require_admin(request):
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    ok = await unblock_ip(ip_address)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "IP not found"})
@app.get("/api/blocked-ips/check/{ip_address}")
async def api_check_ip_blocked(ip_address: str):
    blocked = await is_ip_blocked(ip_address)
    return {"blocked": blocked}
@app.post("/api/fmcsa-register/save")
async def api_save_fmcsa_entries(request: Request):
    body = await request.json()
    entries = body.get("entries", [])
    extracted_date = body.get("extractedDate", "")
    result = await save_fmcsa_register_entries(entries, extracted_date)
    return result
@app.get("/api/fmcsa-register/categories")
async def api_get_fmcsa_categories():
    categories = await get_fmcsa_categories()
    return {"categories": categories}
@app.delete("/api/fmcsa-register/before/{date}")
async def api_delete_fmcsa_before_date(date: str, request: Request):
    if not _require_admin(request):
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    deleted = await delete_fmcsa_entries_before_date(date)
    return {"success": True, "deleted": deleted}
@app.post("/api/new-ventures/scrape")
async def api_scrape_new_ventures(request: Request):
    if not _require_admin(request):
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    body = await request.json()
    added_date = body.get("added_date", "")
    if not added_date:
        return JSONResponse(status_code=400, content={"error": "added_date is required (YYYY-MM-DD)"})
    import datetime
    scrape_date = datetime.date.today().isoformat()
    result = await scrape_broker_snapshot(added_date)
    if not result.get("success"):
        return JSONResponse(status_code=500, content={
            "success": False,
            "error": result.get("error", "Scrape failed"),
        })
    rows = result.get("rows", [])
    db_result = await save_new_venture_entries(rows, scrape_date)
    return {
        "success": True,
        "scraped": len(rows),
        "saved": db_result.get("saved", 0),
        "skipped": db_result.get("skipped", 0),
    }
@app.get("/api/new-ventures")
async def api_fetch_new_ventures(
    docket_number: str = Query(None),
    dot_number: str = Query(None),
    company_name: str = Query(None),
    date_from: str = Query(None),
    date_to: str = Query(None),
    active: str = Query(None),
    state: str = Query(None),
    has_email: str = Query(None),
    carrier_operation: str = Query(None),
    hazmat: str = Query(None),
    power_units_min: str = Query(None),
    power_units_max: str = Query(None),
    drivers_min: str = Query(None),
    drivers_max: str = Query(None),
    bipd_on_file: str = Query(None),
    cargo_on_file: str = Query(None),
    bond_on_file: str = Query(None),
    limit: int = Query(None),
    offset: int = Query(0),
):
    filters = {}
    if docket_number: filters["docket_number"] = docket_number
    if dot_number: filters["dot_number"] = dot_number
    if company_name: filters["company_name"] = company_name
    if date_from: filters["date_from"] = date_from
    if date_to: filters["date_to"] = date_to
    if active: filters["active"] = active
    if state: filters["state"] = state
    if has_email: filters["has_email"] = has_email
    if carrier_operation: filters["carrier_operation"] = carrier_operation
    if hazmat: filters["hazmat"] = hazmat
    if power_units_min: filters["power_units_min"] = power_units_min
    if power_units_max: filters["power_units_max"] = power_units_max
    if drivers_min: filters["drivers_min"] = drivers_min
    if drivers_max: filters["drivers_max"] = drivers_max
    if bipd_on_file: filters["bipd_on_file"] = bipd_on_file
    if cargo_on_file: filters["cargo_on_file"] = cargo_on_file
    if bond_on_file: filters["bond_on_file"] = bond_on_file
    if offset > 0: filters["offset"] = offset
    if limit is not None:
        filters["limit"] = limit
    else:
        has_filters = any(v for k, v in filters.items() if k not in ("limit", "offset"))
        if not has_filters:
            filters["limit"] = 200
    result = await fetch_new_ventures(filters)
    return result
@app.get("/api/new-ventures/count")
async def api_get_new_venture_count():
    count = await get_new_venture_count()
    return {"count": count}
@app.get("/api/new-ventures/dates")
async def api_get_new_venture_dates():
    dates = await get_new_venture_scraped_dates()
    return {"success": True, "dates": dates}
@app.get("/api/new-ventures/detail/{record_id}")
async def api_get_new_venture_detail(record_id: str):
    record = await fetch_new_venture_by_id(record_id)
    if record:
        return record
    return JSONResponse(status_code=404, content={"error": "Record not found"})
@app.delete("/api/new-ventures/{record_id}")
async def api_delete_new_venture(record_id: str, request: Request):
    if not _require_admin(request):
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    ok = await delete_new_venture(record_id)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "Record not found"})
@app.get("/api/carriers/{mc_number}/insurance-history")
async def api_get_insurance_history(mc_number: str):
    policies = await fetch_insurance_history(mc_number)
    return {"success": True, "mc_number": mc_number, "policies": policies, "count": len(policies)}

@app.get("/api/carriers/{dot_number}/inspections")
async def api_get_inspections(dot_number: str):
    inspections = await fetch_inspections(dot_number)
    return {"success": True, "dot_number": dot_number, "inspections": inspections, "count": len(inspections)}
