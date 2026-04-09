import re
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Optional
_DATE_PATTERN = re.compile(r"\d{2}/\d{2}/\d{4}")
def format_date_for_fmcsa(dt: datetime) -> str:
    months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
              "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    day = str(dt.day).zfill(2)
    month = months[dt.month - 1]
    year = str(dt.year)[-2:]
    return f"{day}-{month}-{year}"
async def scrape_fmcsa_register(date_str: Optional[str] = None) -> dict:
    register_date = date_str or format_date_for_fmcsa(datetime.now())
    register_url = "https://li-public.fmcsa.dot.gov/LIVIEW/PKG_register.prc_reg_detail"
    params = f"pd_date={register_date}&pv_vpath=LIVIEW"
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                register_url,
                content=params,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": "https://li-public.fmcsa.dot.gov/LIVIEW/PKG_REGISTER.prc_reg_list",
                    "Origin": "https://li-public.fmcsa.dot.gov",
                },
            )
        if "FMCSA REGISTER" not in resp.text.upper():
            return {
                "success": False,
                "error": "Invalid response from FMCSA. Page might not be available for this date.",
                "entries": [],
            }
        soup = BeautifulSoup(resp.text, "lxml")
        all_entries: list[dict] = []
        categories = [
            {"name": "NAME CHANGE", "anchor": "NC"},
            {"name": "CERTIFICATE, PERMIT, LICENSE", "anchor": "CPL"},
            {"name": "CERTIFICATE OF REGISTRATION", "anchor": "CX2"},
            {"name": "DISMISSAL", "anchor": "DIS"},
            {"name": "WITHDRAWAL", "anchor": "WDN"},
            {"name": "REVOCATION", "anchor": "REV"},
        ]
        for cat in categories:
            section_anchor = soup.find("a", attrs={"name": cat["anchor"]})
            if not section_anchor:
                continue
            parent_table = section_anchor.find_parent("table")
            if not parent_table:
                continue
            target_table = parent_table.find_next_sibling("table")
            if not target_table:
                continue
            for th in target_table.find_all("th", attrs={"scope": "row"}):
                docket = th.get_text(strip=True)
                title_cell = th.find_next_sibling("td")
                if not title_cell:
                    continue
                title_text = " ".join(title_cell.get_text().split()).strip()
                date_val = ""
                immediate_next = title_cell.find_next_sibling("td")
                if immediate_next and _DATE_PATTERN.search(immediate_next.get_text(strip=True)):
                    date_val = immediate_next.get_text(strip=True)
                else:
                    for td in th.find_next_siblings("td"):
                        td_text = td.get_text(strip=True)
                        if _DATE_PATTERN.search(td_text):
                            date_val = td_text
                            break
                if docket and title_cell:
                    all_entries.append({
                        "number": docket,
                        "title": title_text,
                        "decided": date_val or "N/A",
                        "category": cat["name"],
                    })
        seen = set()
        unique_entries = []
        for entry in all_entries:
            key = (entry["number"], entry["title"])
            if key not in seen:
                seen.add(key)
                unique_entries.append(entry)
        return {
            "success": True,
            "count": len(unique_entries),
            "date": register_date,
            "lastUpdated": datetime.now().isoformat(),
            "entries": unique_entries,
        }
    except Exception as e:
        print(f"[FMCSA Register] Scrape error: {e}")
        return {
            "success": False,
            "error": "Failed to scrape FMCSA register data",
            "entries": [],
        }
