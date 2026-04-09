import httpx
import re
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Optional


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

INSURANCE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://searchcarriers.com/",
    "Origin": "https://searchcarriers.com",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

_fmcsa_client: Optional[httpx.AsyncClient] = None
_insurance_client: Optional[httpx.AsyncClient] = None


def _get_fmcsa_client() -> httpx.AsyncClient:
    global _fmcsa_client
    if _fmcsa_client is None or _fmcsa_client.is_closed:
        _fmcsa_client = httpx.AsyncClient(
            timeout=15.0,
            headers=HEADERS,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
    return _fmcsa_client


def _get_insurance_client() -> httpx.AsyncClient:
    global _insurance_client
    if _insurance_client is None or _insurance_client.is_closed:
        _insurance_client = httpx.AsyncClient(
            timeout=15.0,
            headers=INSURANCE_HEADERS,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )
    return _insurance_client


async def close_clients() -> None:
    global _fmcsa_client, _insurance_client
    if _fmcsa_client and not _fmcsa_client.is_closed:
        await _fmcsa_client.aclose()
        _fmcsa_client = None
    if _insurance_client and not _insurance_client.is_closed:
        await _insurance_client.aclose()
        _insurance_client = None


async def fetch_fmcsa(url: str, retries: int = 2, delay_ms: int = 300) -> Optional[str]:
    client = _get_fmcsa_client()
    for attempt in range(retries + 1):
        try:
            resp = await client.get(url)
            if 400 <= resp.status_code < 500:
                return None
            if resp.status_code == 200:
                text = resp.text
                if text and len(text) > 100:
                    return text
        except Exception:
            pass
        if attempt < retries:
            await asyncio.sleep(delay_ms * (attempt + 1) / 1000)
    return None


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ").replace("\n", " ")).strip()


def cf_decode_email(encoded: str) -> str:
    try:
        r = int(encoded[:2], 16)
        email = ""
        for n in range(2, len(encoded), 2):
            email += chr(int(encoded[n : n + 2], 16) ^ r)
        return email
    except Exception:
        return ""


def find_value_by_label(soup: BeautifulSoup, label: str) -> str:
    for th in soup.find_all("th"):
        th_text = clean_text(th.get_text())
        if label in th_text:
            td = th.find_next_sibling("td")
            if td:
                return clean_text(td.get_text())
    return ""


def find_marked_labels(soup: BeautifulSoup, summary: str) -> list[str]:
    table = soup.find("table", attrs={"summary": summary})
    if not table:
        return []
    labels: list[str] = []
    for cell in table.find_all("td"):
        if cell.get_text(strip=True) == "X":
            next_cell = cell.find_next_sibling("td")
            if next_cell:
                labels.append(clean_text(next_cell.get_text()))
    return labels


async def find_dot_email(dot_number: str) -> str:
    if not dot_number:
        return ""
    html = await fetch_fmcsa(
        f"https://ai.fmcsa.dot.gov/SMS/Carrier/{dot_number}/CarrierRegistration.aspx"
    )
    if not html:
        return ""

    soup = BeautifulSoup(html, "lxml")

    for label_tag in soup.find_all("label"):
        label_text = label_tag.get_text() or ""
        if "Email:" not in label_text:
            continue

        parent = label_tag.parent
        if parent:
            cf_el = parent.find(attrs={"data-cfemail": True})
            if cf_el:
                return cf_decode_email(cf_el.get("data-cfemail", ""))
            parent_text = clean_text(parent.get_text().replace("Email:", ""))
            if parent_text and "@" in parent_text:
                return parent_text

        sibling = label_tag.find_next_sibling()
        if sibling:
            if sibling.get("data-cfemail"):
                return cf_decode_email(sibling["data-cfemail"])
            cf_child = sibling.find(attrs={"data-cfemail": True})
            if cf_child:
                return cf_decode_email(cf_child.get("data-cfemail", ""))
            txt = clean_text(sibling.get_text())
            if txt and len(txt) > 4 and "email protected" not in txt.lower():
                return txt
    return ""


async def fetch_safety_data(dot: str) -> dict:
    if not dot:
        return {"rating": "N/A", "ratingDate": "", "basicScores": [], "oosRates": []}

    html = await fetch_fmcsa(
        f"https://ai.fmcsa.dot.gov/SMS/Carrier/{dot}/CompleteProfile.aspx"
    )
    if not html:
        return {"rating": "N/A", "ratingDate": "", "basicScores": [], "oosRates": []}

    soup = BeautifulSoup(html, "lxml")

    rating_el = soup.find(id="Rating")
    rating = clean_text(rating_el.get_text()) if rating_el else "NOT RATED"

    rating_date_el = soup.find(id="RatingDate")
    rating_date = ""
    if rating_date_el:
        rd = clean_text(rating_date_el.get_text())
        rating_date = re.sub(r"Rating Date:|[()]", "", rd).strip()

    categories = [
        "Unsafe Driving", "Crash Indicator", "HOS Compliance",
        "Vehicle Maintenance", "Controlled Substances", "Hazmat Compliance", "Driver Fitness",
    ]
    basic_scores: list[dict] = []
    sum_data_row = soup.find("tr", class_="sumData")
    if sum_data_row:
        cells = sum_data_row.find_all("td")
        for i, cell in enumerate(cells):
            if i < len(categories):
                val_span = cell.find("span", class_="val")
                val = clean_text(val_span.get_text()) if val_span else clean_text(cell.get_text())
                basic_scores.append({"category": categories[i], "measure": val or "0.00"})

    oos_rates: list[dict] = []
    safety_rating_div = soup.find(id="SafetyRating")
    if safety_rating_div:
        oos_table = safety_rating_div.find("table")
        if oos_table:
            tbody = oos_table.find("tbody")
            if tbody:
                for row in tbody.find_all("tr"):
                    cols = row.find_all(["th", "td"])
                    if len(cols) >= 3:
                        type_text = clean_text(cols[0].get_text())
                        if type_text and type_text != "Type":
                            oos_rates.append({
                                "type": type_text,
                                "rate": clean_text(cols[1].get_text()),
                                "nationalAvg": clean_text(cols[2].get_text()),
                            })

    return {
        "rating": rating,
        "ratingDate": rating_date,
        "basicScores": basic_scores,
        "oosRates": oos_rates,
    }


async def fetch_inspection_and_crash_data(dot: str) -> dict:
    if not dot:
        return {"inspections": [], "crashes": []}

    html = await fetch_fmcsa(
        f"https://ai.fmcsa.dot.gov/SMS/Carrier/{dot}/CompleteProfile.aspx"
    )
    if not html:
        return {"inspections": [], "crashes": []}

    soup = BeautifulSoup(html, "lxml")
    inspections: list[dict] = []
    crashes: list[dict] = []

    try:
        i_table = soup.find("table", id="inspectionTable")
        if i_table:
            i_tbody = i_table.find("tbody", class_="dataBody")
            if i_tbody:
                current_report = None
                for row in i_tbody.find_all("tr"):
                    row_classes = " ".join(row.get("class", []))
                    if "inspection" in row_classes:
                        if current_report:
                            inspections.append(current_report)
                        cols = row.find_all("td")
                        if len(cols) >= 3:
                            current_report = {
                                "reportNumber": clean_text(cols[1].get_text()),
                                "location": clean_text(cols[2].get_text()),
                                "date": clean_text(cols[0].get_text()),
                                "oosViolations": 0,
                                "driverViolations": 0,
                                "vehicleViolations": 0,
                                "hazmatViolations": 0,
                                "violationList": [],
                            }
                    elif "viol" in row_classes and current_report:
                        label_el = row.find("label")
                        label_text = clean_text(label_el.get_text()) if label_el else ""
                        desc_el = row.find("span", class_="violCodeDesc")
                        desc = clean_text(desc_el.get_text()) if desc_el else ""
                        weight_el = row.find("td", class_="weight")
                        weight = clean_text(weight_el.get_text()) if weight_el else ""
                        current_report["violationList"].append({
                            "label": label_text,
                            "description": desc,
                            "weight": weight,
                        })
                        label_lower = label_text.lower()
                        if "oos" in row_classes or "(oos)" in desc.lower():
                            current_report["oosViolations"] += 1
                        if "vehicle maint" in label_lower:
                            current_report["vehicleViolations"] += 1
                        elif any(t in label_lower for t in ["driver fitness", "unsafe driving", "hos compliance", "drugs/alcohol"]):
                            current_report["driverViolations"] += 1
                        elif "hazmat" in label_lower or "hm compliance" in label_lower:
                            current_report["hazmatViolations"] += 1
                        else:
                            current_report["vehicleViolations"] += 1
                if current_report:
                    inspections.append(current_report)

        c_table = soup.find("table", id="crashTable")
        if c_table:
            c_tbody = c_table.find("tbody", class_="dataBody")
            if c_tbody:
                for row in c_tbody.find_all("tr", class_="crash"):
                    cols = row.find_all("td")
                    if len(cols) >= 7:
                        crashes.append({
                            "date": clean_text(cols[0].get_text()),
                            "number": clean_text(cols[1].get_text()),
                            "state": clean_text(cols[2].get_text()),
                            "plateNumber": clean_text(cols[3].get_text()),
                            "plateState": clean_text(cols[4].get_text()),
                            "fatal": clean_text(cols[5].get_text()),
                            "injuries": clean_text(cols[6].get_text()),
                        })
    except Exception as e:
        print(f"Error parsing inspection/crash data: {e}")

    return {"inspections": inspections, "crashes": crashes}


async def fetch_insurance_data(dot: str) -> dict:
    if not dot:
        return {"policies": [], "raw": None}

    urls_to_try = [
        f"https://searchcarriers.com/company/{dot}/insurances",
        f"https://searchcarriers.com/api/company/{dot}/insurances",
    ]

    client = _get_insurance_client()
    result = None
    for target_url in urls_to_try:
        if result is not None:
            break
        for attempt in range(2):
            try:
                resp = await client.get(target_url)
                if 400 <= resp.status_code < 500:
                    break
                if resp.status_code == 200:
                    text = resp.text.strip()
                    if text and (text.startswith("{") or text.startswith("[")):
                        try:
                            result = resp.json()
                            break
                        except Exception:
                            pass
            except Exception:
                pass
            if attempt < 1:
                await asyncio.sleep(0.1 * (attempt + 1))

    if result is None:
        return {"policies": [], "raw": None}

    raw_data = result.get("data", result if isinstance(result, list) else [])
    policies: list[dict] = []

    if isinstance(raw_data, list):
        for p in raw_data:
            carrier_name = str(
                p.get("name_company")
                or p.get("insurance_company")
                or p.get("insurance_company_name")
                or p.get("company_name")
                or "NOT SPECIFIED"
            ).upper()

            policy_number = str(
                p.get("policy_no") or p.get("policy_number") or p.get("pol_num") or "N/A"
            ).upper()

            eff_date_raw = p.get("effective_date", "")
            effective_date = eff_date_raw.split(" ")[0] if eff_date_raw else "N/A"

            coverage = p.get("max_cov_amount") or p.get("coverage_to") or p.get("coverage_amount") or "N/A"
            if coverage != "N/A":
                try:
                    num = float(coverage)
                    if 0 < num < 10000:
                        coverage = f"${int(num * 1000):,}"
                    else:
                        coverage = f"${int(num):,}"
                except (ValueError, TypeError):
                    pass

            ins_type = str(p.get("ins_type_code", "N/A"))
            if ins_type == "1":
                ins_type = "BI&PD"
            elif ins_type == "2":
                ins_type = "CARGO"
            elif ins_type == "3":
                ins_type = "BOND"

            ins_class = str(p.get("ins_class_code", "N/A")).upper()
            if ins_class == "P":
                ins_class = "PRIMARY"
            elif ins_class == "E":
                ins_class = "EXCESS"

            policies.append({
                "dot": dot,
                "carrier": carrier_name,
                "policyNumber": policy_number,
                "effectiveDate": effective_date,
                "coverageAmount": str(coverage),
                "type": ins_type,
                "class": ins_class,
            })

    return {"policies": policies, "raw": result}


async def scrape_carrier(mc_number: str) -> Optional[dict]:
    html = await fetch_fmcsa(
        f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=MC_MX&query_string={mc_number}"
    )
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    if not soup.find("center"):
        return None

    get_val = lambda label: find_value_by_label(soup, label)

    dot_number = get_val("USDOT Number:")

    status = get_val("Operating Authority Status:")
    status = re.sub(r"(\*Please Note|Please Note|For Licensing)[\s\S]*", "", status, flags=re.IGNORECASE).strip()
    status = re.sub(r"\s+", " ", status)

    if dot_number:
        email_task = find_dot_email(dot_number)
        safety_task = fetch_safety_data(dot_number)
        inspection_task = fetch_inspection_and_crash_data(dot_number)
        email, safety, insp_data = await asyncio.gather(email_task, safety_task, inspection_task)
    else:
        email, safety, insp_data = "", None, None

    clean_email = re.sub(r"[\[\]Â]", "", email).strip()
    if "email protected" in clean_email.lower():
        clean_email = ""

    return {
        "mcNumber": mc_number,
        "dotNumber": dot_number,
        "legalName": get_val("Legal Name:"),
        "dbaName": get_val("DBA Name:"),
        "entityType": get_val("Entity Type:"),
        "status": status,
        "email": clean_email,
        "phone": get_val("Phone:"),
        "powerUnits": get_val("Power Units:"),
        "nonCmvUnits": get_val("Non-CMV Units:"),
        "drivers": get_val("Drivers:"),
        "physicalAddress": get_val("Physical Address:"),
        "mailingAddress": get_val("Mailing Address:"),
        "dateScraped": datetime.now().strftime("%m/%d/%Y"),
        "mcs150Date": get_val("MCS-150 Form Date:"),
        "mcs150Mileage": get_val("MCS-150 Mileage (Year):"),
        "operationClassification": find_marked_labels(soup, "Operation Classification"),
        "carrierOperation": find_marked_labels(soup, "Carrier Operation"),
        "cargoCarried": find_marked_labels(soup, "Cargo Carried"),
        "outOfServiceDate": get_val("Out of Service Date:"),
        "stateCarrierId": get_val("State Carrier ID Number:"),
        "dunsNumber": get_val("DUNS Number:"),
        "safetyRating": safety["rating"] if safety else "NOT RATED",
        "safetyRatingDate": safety["ratingDate"] if safety else "",
        "basicScores": safety["basicScores"] if safety else [],
        "oosRates": safety["oosRates"] if safety else [],
        "inspections": insp_data["inspections"] if insp_data else [],
        "crashes": insp_data["crashes"] if insp_data else [],
    }
