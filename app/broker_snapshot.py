import asyncio
import csv
import io
import os
import time
from curl_cffi import requests as cffi_requests
BASE_URL = "https://brokersnapshot.com"
def _get_proxies() -> dict:
    proxy_url = os.getenv("BROKER_SNAPSHOT_PROXY", "")
    if proxy_url:
        return {"http": proxy_url, "https": proxy_url}
    return {}
_CSV_TO_DB: dict[str, str] = {
    "dot_number": "dot_number",
    "prefix": "prefix",
    "docket_number": "docket_number",
    "status_code": "status_code",
    "carship": "carship",
    "carrier_operation": "carrier_operation",
    "name": "name",
    "name_dba": "name_dba",
    "add_date": "add_date",
    "chgn_date": "chgn_date",
    "common_stat": "common_stat",
    "contract_stat": "contract_stat",
    "broker_stat": "broker_stat",
    "common_app_pend": "common_app_pend",
    "contract_app_pend": "contract_app_pend",
    "broker_app_pend": "broker_app_pend",
    "common_rev_pend": "common_rev_pend",
    "contract_rev_pend": "contract_rev_pend",
    "broker_rev_pend": "broker_rev_pend",
    "property_chk": "property_chk",
    "passenger_chk": "passenger_chk",
    "hhg_chk": "hhg_chk",
    "private_auth_chk": "private_auth_chk",
    "enterprise_chk": "enterprise_chk",
    "operatingstatus": "operating_status",
    "operatingstatusindicator": "operating_status_indicator",
    "phy_str": "phy_str",
    "phy_city": "phy_city",
    "phy_st": "phy_st",
    "phy_zip": "phy_zip",
    "phy_country": "phy_country",
    "phy_cnty": "phy_cnty",
    "mai_str": "mai_str",
    "mai_city": "mai_city",
    "mai_st": "mai_st",
    "mai_zip": "mai_zip",
    "mai_country": "mai_country",
    "mai_cnty": "mai_cnty",
    "phy_undeliv": "phy_undeliv",
    "mai_undeliv": "mai_undeliv",
    "phy_phone": "phy_phone",
    "phy_fax": "phy_fax",
    "mai_phone": "mai_phone",
    "mai_fax": "mai_fax",
    "cell_phone": "cell_phone",
    "email_address": "email_address",
    "company_officer_1": "company_officer_1",
    "company_officer_2": "company_officer_2",
    "genfreight": "genfreight",
    "household": "household",
    "metalsheet": "metalsheet",
    "motorveh": "motorveh",
    "drivetow": "drivetow",
    "logpole": "logpole",
    "bldgmat": "bldgmat",
    "mobilehome": "mobilehome",
    "machlrg": "machlrg",
    "produce": "produce",
    "liqgas": "liqgas",
    "intermodal": "intermodal",
    "passengers": "passengers",
    "oilfield": "oilfield",
    "livestock": "livestock",
    "grainfeed": "grainfeed",
    "coalcoke": "coalcoke",
    "meat": "meat",
    "garbage": "garbage",
    "usmail": "usmail",
    "chem": "chem",
    "drybulk": "drybulk",
    "coldfood": "coldfood",
    "beverages": "beverages",
    "paperprod": "paperprod",
    "utility": "utility",
    "farmsupp": "farmsupp",
    "construct": "construct",
    "waterwell": "waterwell",
    "cargoothr": "cargoothr",
    "cargoothr_desc": "cargoothr_desc",
    "hm_ind": "hm_ind",
    "bipd_req": "bipd_req",
    "cargo_req": "cargo_req",
    "bond_req": "bond_req",
    "bipd_file": "bipd_file",
    "cargo_file": "cargo_file",
    "bond_file": "bond_file",
    "owntruck": "owntruck",
    "owntract": "owntract",
    "owntrail": "owntrail",
    "owncoach": "owncoach",
    "ownschool_1_8": "ownschool_1_8",
    "ownschool_9_15": "ownschool_9_15",
    "ownschool_16": "ownschool_16",
    "ownbus_16": "ownbus_16",
    "ownvan_1_8": "ownvan_1_8",
    "ownvan_9_15": "ownvan_9_15",
    "ownlimo_1_8": "ownlimo_1_8",
    "ownlimo_9_15": "ownlimo_9_15",
    "ownlimo_16": "ownlimo_16",
    "trmtruck": "trmtruck",
    "trmtract": "trmtract",
    "trmtrail": "trmtrail",
    "trmcoach": "trmcoach",
    "trmschool_1_8": "trmschool_1_8",
    "trmschool_9_15": "trmschool_9_15",
    "trmschool_16": "trmschool_16",
    "trmbus_16": "trmbus_16",
    "trmvan_1_8": "trmvan_1_8",
    "trmvan_9_15": "trmvan_9_15",
    "trmlimo_1_8": "trmlimo_1_8",
    "trmlimo_9_15": "trmlimo_9_15",
    "trmlimo_16": "trmlimo_16",
    "trptruck": "trptruck",
    "trptract": "trptract",
    "trptrail": "trptrail",
    "trpcoach": "trpcoach",
    "trpschool_1_8": "trpschool_1_8",
    "trpschool_9_15": "trpschool_9_15",
    "trpschool_16": "trpschool_16",
    "trpbus_16": "trpbus_16",
    "trpvan_1_8": "trpvan_1_8",
    "trpvan_9_15": "trpvan_9_15",
    "trplimo_1_8": "trplimo_1_8",
    "trplimo_9_15": "trplimo_9_15",
    "trplimo_16": "trplimo_16",
    "total_trucks": "total_trucks",
    "total_buses": "total_buses",
    "total_pwr": "total_pwr",
    "fleetsize": "fleetsize",
    "inter_within_100": "inter_within_100",
    "inter_beyond_100": "inter_beyond_100",
    "total_inter_drivers": "total_inter_drivers",
    "intra_within_100": "intra_within_100",
    "intra_beyond_100": "intra_beyond_100",
    "total_intra_drivers": "total_intra_drivers",
    "total_drivers": "total_drivers",
    "avg_tld": "avg_tld",
    "total_cdl": "total_cdl",
    "review_type": "review_type",
    "review_id": "review_id",
    "review_date": "review_date",
    "recordable_crash_rate": "recordable_crash_rate",
    "mcs150_mileage": "mcs150_mileage",
    "mcs151_mileage": "mcs151_mileage",
    "mcs150_mileage_year": "mcs150_mileage_year",
    "mcs150_date": "mcs150_date",
    "safety_rating": "safety_rating",
    "safety_rating_date": "safety_rating_date",
    "arber": "arber",
    "smartway": "smartway",
    "tia": "tia",
    "tiaphone": "tia_phone",
    "tiacontactname": "tia_contact_name",
    "tiatoolfree": "tia_tool_free",
    "tiafax": "tia_fax",
    "tiaemail": "tia_email",
    "tiawebsite": "tia_website",
    "phy_upsstore": "phy_ups_store",
    "mai_upsstore": "mai_ups_store",
    "phy_mailbox": "phy_mail_box",
    "mai_mailbox": "mai_mail_box",
}
def _normalise_row(row: dict) -> dict:
    mapped: dict = {}
    raw_data: dict = {}
    for csv_key, value in row.items():
        raw_data[csv_key] = value
        clean_key = csv_key.strip().lower()
        db_key = _CSV_TO_DB.get(clean_key)
        if db_key:
            mapped[db_key] = value.strip() if isinstance(value, str) and value else value
    mapped["raw_data"] = raw_data
    return mapped
def _scrape_sync(added_date: str, progress_cb=None) -> dict:
    proxies = _get_proxies()
    session = cffi_requests.Session(impersonate="chrome")
    email = os.getenv("BROKER_SNAPSHOT_EMAIL", "")
    password = os.getenv("BROKER_SNAPSHOT_PASSWORD", "")
    if not email or not password:
        return {"success": False, "error": "BROKER_SNAPSHOT_EMAIL / BROKER_SNAPSHOT_PASSWORD env vars not set"}
    def _report(pct: int, msg: str):
        if progress_cb:
            progress_cb(pct, msg)
        print(f"[BrokerSnapshot] {pct}% — {msg}")
    try:
        _report(5, "Loading login page…")
        session.get(f"{BASE_URL}/LogIn", proxies=proxies, timeout=30)
        time.sleep(2)
        _report(10, "Logging in…")
        login_data = {"email": email, "password": password, "ReturnUrl": ""}
        resp = session.post(
            f"{BASE_URL}/LogIn",
            data=login_data,
            proxies=proxies,
            allow_redirects=True,
            timeout=30,
        )
        if "LogOff" not in resp.text:
            return {"success": False, "error": "Login failed — check credentials or proxy"}
        _report(20, "Login successful, starting export…")
        export_params = {"added-date": added_date}
        resp = session.get(
            f"{BASE_URL}/SearchCompanies/GenerateExport",
            params=export_params,
            proxies=proxies,
            timeout=30,
        )
        result = resp.json()
        if not result.get("Success"):
            return {"success": False, "error": f"Export trigger failed: {result.get('Message', 'unknown')}"}
        _report(30, "Export started, polling for completion…")
        key = None
        max_polls = 120
        for poll in range(max_polls):
            resp = session.get(
                f"{BASE_URL}/SearchCompanies/GetStatusExport",
                params=export_params,
                proxies=proxies,
                timeout=30,
            )
            d = resp.json().get("Data")
            if d and d.get("FileName"):
                key = d["FileName"]
                break
            if d and d.get("Percent"):
                pct = min(30 + int(d["Percent"] * 0.5), 80)
                _report(pct, f"Export {d['Percent']}% complete…")
            time.sleep(5)
        if not key:
            return {"success": False, "error": "Export timed out after polling"}
        _report(85, "Downloading CSV…")
        resp = session.get(
            f"{BASE_URL}/SearchCompanies/DownloadExport",
            params={"Key": key},
            proxies=proxies,
            timeout=60,
        )
        _report(90, "Parsing CSV…")
        content = resp.content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(content))
        rows = [_normalise_row(row) for row in reader]
        for row in rows:
            if not row.get("add_date"):
                row["add_date"] = added_date
        _report(100, f"Done — {len(rows)} records parsed")
        return {"success": True, "rows": rows, "count": len(rows)}
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        session.close()
async def scrape_broker_snapshot(added_date: str, progress_cb=None) -> dict:
    return await asyncio.to_thread(_scrape_sync, added_date, progress_cb)
