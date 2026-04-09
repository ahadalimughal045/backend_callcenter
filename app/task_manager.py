import asyncio
import uuid
from datetime import datetime, timezone
from typing import Optional
from app.scraper import scrape_carrier, fetch_insurance_data
from app.database import upsert_carrier, update_carrier_insurance

_MAX_COMPLETED_TASKS = 20
class TaskManager:
    def __init__(self):
        self.tasks: dict[str, dict] = {}
        self._running_tasks: dict[str, asyncio.Task] = {}
    async def start_scraper_task(self, config: dict) -> str:
        task_id = str(uuid.uuid4())[:8]
        start_point = int(config.get("startPoint", "1580000"))
        record_count = int(config.get("recordCount", 50))
        include_carriers = config.get("includeCarriers", True)
        include_brokers = config.get("includeBrokers", False)
        only_authorized = config.get("onlyAuthorized", True)
        self.tasks[task_id] = {
            "id": task_id,
            "type": "scraper",
            "status": "running",
            "config": config,
            "progress": 0,
            "completed": 0,
            "total": record_count,
            "extracted": 0,
            "dbSaved": 0,
            "failed": 0,
            "logs": [
                f"[{self._now()}] Task {task_id} started",
                f"[{self._now()}] Targeting {record_count} records starting at MC# {start_point}",
                f"[{self._now()}] Filters: carriers={include_carriers}, brokers={include_brokers}, authorized_only={only_authorized}",
            ],
            "scrapedData": [],
            "startedAt": datetime.now(timezone.utc).isoformat(),
            "stoppedAt": None,
        }
        async_task = asyncio.create_task(
            self._run_scraper(task_id, start_point, record_count,
                              include_carriers, include_brokers, only_authorized)
        )
        self._running_tasks[task_id] = async_task
        return task_id
    async def start_insurance_task(self, config: dict) -> str:
        task_id = str(uuid.uuid4())[:8]
        dot_numbers = config.get("dotNumbers", [])
        self.tasks[task_id] = {
            "id": task_id,
            "type": "insurance",
            "status": "running",
            "config": config,
            "progress": 0,
            "completed": 0,
            "total": len(dot_numbers),
            "insFound": 0,
            "dbSaved": 0,
            "failed": 0,
            "logs": [
                f"[{self._now()}] 🚀 Insurance task {task_id} started",
                f"[{self._now()}] 🔍 Targeting {len(dot_numbers)} DOT records",
            ],
            "startedAt": datetime.now(timezone.utc).isoformat(),
            "stoppedAt": None,
        }
        async_task = asyncio.create_task(
            self._run_insurance(task_id, dot_numbers)
        )
        self._running_tasks[task_id] = async_task
        return task_id
    def stop_task(self, task_id: str):
        task = self.tasks.get(task_id)
        if not task:
            return
        if task["status"] in ("completed", "stopped"):
            self._add_log(task_id, "Stop received but task already finished.")
            return
        task["status"] = "stopping"
        self._add_log(task_id, "Stop signal received. Finishing current operation...")
        if task_id in self._running_tasks:
            self._running_tasks[task_id].cancel()
    def get_task_status(self, task_id: str) -> Optional[dict]:
        task = self.tasks.get(task_id)
        if not task:
            return None
        result = {k: v for k, v in task.items() if k != "scrapedData"}
        result["scrapedCount"] = len(task.get("scrapedData", []))
        result["recentData"] = task.get("scrapedData", [])[-20:]
        result["logs"] = task.get("logs", [])[-100:]
        return result
    def get_task_data(self, task_id: str) -> Optional[list]:
        task = self.tasks.get(task_id)
        if not task:
            return None
        return task.get("scrapedData", [])
    def get_active_task_id(self, task_type: str) -> Optional[str]:
        candidates = [(tid, t) for tid, t in self.tasks.items() if t.get("type") == task_type]
        if not candidates:
            return None
        active = [c for c in candidates if c[1].get("status") in ("running", "stopping")]
        if active:
            return active[-1][0]
        return None
    def list_tasks(self) -> list[dict]:
        result = []
        for task_id, task in self.tasks.items():
            result.append({
                "id": task_id,
                "type": task.get("type"),
                "status": task.get("status"),
                "progress": task.get("progress"),
                "startedAt": task.get("startedAt"),
                "stoppedAt": task.get("stoppedAt"),
            })
        return result
    async def _run_scraper(self, task_id: str, start: int, total: int,
                           include_carriers: bool, include_brokers: bool,
                           only_authorized: bool):
        task = self.tasks[task_id]
        completed = 0
        extracted = 0
        db_saved = 0
        failed = 0
        batch_buffer: list[dict] = []
        BATCH_SIZE = 50
        try:
            for i in range(total):
                if task["status"] == "stopping":
                    break
                mc = str(start + i)
                self._add_log(task_id, f"Scraping MC# {mc} ({i+1}/{total})...")
                try:
                    data = await scrape_carrier(mc)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._add_log(task_id, f"[Error] MC {mc}: {str(e)[:100]}")
                    data = None
                completed += 1
                if data:
                    entity_type = (data.get("entityType") or "").upper()
                    status_text = (data.get("status") or "").upper()
                    is_carrier = "CARRIER" in entity_type
                    is_broker = "BROKER" in entity_type
                    matches_filter = True
                    if not include_carriers and is_carrier and not is_broker:
                        matches_filter = False
                    if not include_brokers and is_broker and not is_carrier:
                        matches_filter = False
                    if only_authorized:
                        if "NOT AUTHORIZED" in status_text or "AUTHORIZED" not in status_text:
                            matches_filter = False
                    if matches_filter:
                        extracted += 1
                        task["scrapedData"].append(data)
                        batch_buffer.append(data)
                        self._add_log(task_id, f"[Success] MC {mc}: {data.get('legalName', 'Unknown')}")
                        if len(batch_buffer) >= BATCH_SIZE:
                            saved = await self._save_batch_to_db(batch_buffer)
                            db_saved += saved
                            self._add_log(task_id, f"DB Sync: {saved}/{len(batch_buffer)} records saved")
                            batch_buffer = []
                    else:
                        self._add_log(task_id, f"[Filtered] MC {mc}: {data.get('legalName', '')} (didn't match filters)")
                else:
                    failed += 1
                    self._add_log(task_id, f"[No Data] MC {mc}")
                task["completed"] = completed
                task["extracted"] = extracted
                task["dbSaved"] = db_saved
                task["failed"] = failed
                task["progress"] = round((completed / total) * 100)
        except asyncio.CancelledError:
            self._add_log(task_id, "Task cancelled. Saving remaining batch...")
        if batch_buffer:
            try:
                saved = await self._save_batch_to_db(batch_buffer)
                db_saved += saved
                task["dbSaved"] = db_saved
                self._add_log(task_id, f"Final sync: {saved} records saved")
            except Exception:
                self._add_log(task_id, "Failed to save final batch")
        task["status"] = "stopped" if task["status"] == "stopping" else "completed"
        task["stoppedAt"] = datetime.now(timezone.utc).isoformat()
        task["scrapedData"] = []
        self._add_log(task_id, f"Task finished. Extracted: {extracted}, DB saved: {db_saved}, Failed: {failed}")
        if task_id in self._running_tasks:
            del self._running_tasks[task_id]
        self._cleanup_old_tasks()
    async def _run_insurance(self, task_id: str, dot_numbers: list[str]):
        task = self.tasks[task_id]
        ins_found = 0
        db_saved_count = 0
        failed_count = 0
        REQUEST_DELAY = 0.333
        try:
            for i, dot in enumerate(dot_numbers):
                if task["status"] == "stopping":
                    break
                self._add_log(task_id, f"[INSURANCE] [{i+1}/{len(dot_numbers)}] Querying DOT: {dot}...")
                try:
                    result = await fetch_insurance_data(dot)
                    policies = result.get("policies", [])
                    if policies:
                        ins_found += 1
                        self._add_log(task_id, f"Success: {len(policies)} insurance filings for {dot}")
                        try:
                            updated = await update_carrier_insurance(dot, policies)
                            if updated:
                                db_saved_count += 1
                                self._add_log(task_id, f"DB Sync: DOT {dot} updated")
                            else:
                                self._add_log(task_id, f"DOT {dot} not in carriers collection, skipping DB save")
                        except Exception:
                            self._add_log(task_id, f"DB Fail: Could not sync {dot}")
                    else:
                        self._add_log(task_id, f"No insurance found for DOT {dot}")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    failed_count += 1
                    self._add_log(task_id, f"Fail: Insurance timeout for DOT {dot}")
                task["completed"] = i + 1
                task["insFound"] = ins_found
                task["dbSaved"] = db_saved_count
                task["failed"] = failed_count
                task["progress"] = round(((i + 1) / len(dot_numbers)) * 100) if dot_numbers else 100
                await asyncio.sleep(REQUEST_DELAY)
        except asyncio.CancelledError:
            self._add_log(task_id, "Insurance task cancelled.")
        task["status"] = "stopped" if task["status"] == "stopping" else "completed"
        task["stoppedAt"] = datetime.now(timezone.utc).isoformat()
        self._add_log(task_id, "Insurance enrichment complete. Database synchronized.")
        if task_id in self._running_tasks:
            del self._running_tasks[task_id]
        self._cleanup_old_tasks()
    async def _save_batch_to_db(self, batch: list[dict]) -> int:
        records = []
        for carrier in batch:
            records.append({
                "mc_number": carrier.get("mcNumber"),
                "dot_number": carrier.get("dotNumber"),
                "legal_name": carrier.get("legalName"),
                "dba_name": carrier.get("dbaName"),
                "entity_type": carrier.get("entityType"),
                "status": carrier.get("status"),
                "email": carrier.get("email"),
                "phone": carrier.get("phone"),
                "power_units": carrier.get("powerUnits"),
                "drivers": carrier.get("drivers"),
                "non_cmv_units": carrier.get("nonCmvUnits"),
                "physical_address": carrier.get("physicalAddress"),
                "mailing_address": carrier.get("mailingAddress"),
                "date_scraped": carrier.get("dateScraped"),
                "mcs150_date": carrier.get("mcs150Date"),
                "mcs150_mileage": carrier.get("mcs150Mileage"),
                "operation_classification": carrier.get("operationClassification", []),
                "carrier_operation": carrier.get("carrierOperation", []),
                "cargo_carried": carrier.get("cargoCarried", []),
                "out_of_service_date": carrier.get("outOfServiceDate"),
                "state_carrier_id": carrier.get("stateCarrierId"),
                "duns_number": carrier.get("dunsNumber"),
                "safety_rating": carrier.get("safetyRating"),
                "safety_rating_date": carrier.get("safetyRatingDate"),
                "basic_scores": carrier.get("basicScores"),
                "oos_rates": carrier.get("oosRates"),
                "insurance_policies": carrier.get("insurancePolicies"),
                "inspections": carrier.get("inspections"),
                "crashes": carrier.get("crashes"),
            })
        results = await asyncio.gather(
            *(upsert_carrier(r) for r in records),
            return_exceptions=True,
        )
        return sum(1 for ok in results if ok is True)
    def _add_log(self, task_id: str, message: str):
        if task_id in self.tasks:
            logs = self.tasks[task_id]["logs"]
            logs.append(f"[{self._now()}] {message}")
            if len(logs) > 500:
                self.tasks[task_id]["logs"] = logs[-500:]
    def _cleanup_old_tasks(self):
        completed = [
            (tid, t) for tid, t in self.tasks.items()
            if t.get("status") in ("completed", "stopped")
        ]
        if len(completed) > _MAX_COMPLETED_TASKS:
            completed.sort(key=lambda x: x[1].get("stoppedAt") or "")
            for tid, _ in completed[:len(completed) - _MAX_COMPLETED_TASKS]:
                del self.tasks[tid]
                self._running_tasks.pop(tid, None)
    def _now(self) -> str:
        return datetime.now(timezone.utc).strftime("%H:%M:%S")
# Singleton instance
task_manager = TaskManager()
