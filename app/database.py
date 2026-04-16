import os
import json
import asyncio
import asyncpg
from typing import Optional
from dotenv import load_dotenv
 # Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    import warnings
    warnings.warn("DATABASE_URL is not set. Database connections will fail.")

_pool: Optional[asyncpg.Pool] = None


_SCHEMA_SQL = """
-- ── Helpers ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION extract_state(addr TEXT) RETURNS TEXT AS $$
DECLARE parts TEXT[];
BEGIN
    IF addr IS NULL THEN RETURN NULL; END IF;
    parts := string_to_array(addr, ',');
    IF array_length(parts, 1) < 2 THEN RETURN NULL; END IF;
    RETURN trim(split_part(trim(parts[array_length(parts, 1)]), ' ', 1));
EXCEPTION WHEN OTHERS THEN RETURN NULL; END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION text_to_int(t TEXT) RETURNS INTEGER AS $$
BEGIN
    IF t ~ '^[0-9]+$' THEN RETURN t::integer; END IF;
    RETURN 0;
EXCEPTION WHEN OTHERS THEN RETURN 0; END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ── Tables ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS carriers (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    mc_number TEXT NOT NULL UNIQUE,
    dot_number TEXT NOT NULL,
    legal_name TEXT NOT NULL,
    dba_name TEXT,
    entity_type TEXT,
    status TEXT,
    email TEXT,
    phone TEXT,
    power_units TEXT,
    drivers TEXT,
    non_cmv_units TEXT,
    physical_address TEXT,
    mailing_address TEXT,
    date_scraped TEXT,
    mcs150_date TEXT,
    mcs150_mileage TEXT,
    operation_classification TEXT[],
    carrier_operation TEXT[],
    cargo_carried TEXT[],
    out_of_service_date TEXT,
    state_carrier_id TEXT,
    duns_number TEXT,
    safety_rating TEXT,
    safety_rating_date TEXT,
    basic_scores JSONB,
    oos_rates JSONB,
    insurance_policies JSONB,
    crashes JSONB,
    inspections JSONB,
    physical_state TEXT,
    power_units_int INTEGER DEFAULT 0,
    drivers_int INTEGER DEFAULT 0,
    crash_count INTEGER DEFAULT 0,
    inspection_count INTEGER DEFAULT 0,
    oos_violation_total INTEGER DEFAULT 0,
    injuries_total INTEGER DEFAULT 0,
    fatalities_total INTEGER DEFAULT 0,
    towaway_total INTEGER DEFAULT 0,
    years_in_business FLOAT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fmcsa_register (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    number TEXT NOT NULL,
    title TEXT NOT NULL,
    decided TEXT,
    category TEXT,
    date_fetched TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(number, date_fetched)
);

CREATE TABLE IF NOT EXISTS users (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')),
    plan TEXT NOT NULL DEFAULT 'Free' CHECK (plan IN ('Free', 'Starter', 'Pro', 'Enterprise')),
    daily_limit INTEGER NOT NULL DEFAULT 50,
    records_extracted_today INTEGER NOT NULL DEFAULT 0,
    last_active TEXT DEFAULT 'Never',
    ip_address TEXT,
    is_online BOOLEAN DEFAULT false,
    is_blocked BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS blocked_ips (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ip_address TEXT NOT NULL UNIQUE,
    reason TEXT,
    blocked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    blocked_by TEXT
);

CREATE TABLE IF NOT EXISTS insurance_history (
    id SERIAL PRIMARY KEY,
    docket_number VARCHAR(20),
    dot_number VARCHAR(20),
    ins_form_code VARCHAR(10),
    ins_type_desc VARCHAR(50),
    name_company VARCHAR(100),
    policy_no VARCHAR(50),
    trans_date VARCHAR(15),
    underl_lim_amount VARCHAR(15),
    max_cov_amount VARCHAR(15),
    effective_date VARCHAR(15),
    cancl_effective_date VARCHAR(15)
);

-- ── Indexes ─────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_carriers_mc_number ON carriers(mc_number);
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number ON carriers(dot_number);
CREATE INDEX IF NOT EXISTS idx_carriers_created_at ON carriers(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_carriers_status ON carriers(status);
CREATE INDEX IF NOT EXISTS idx_carriers_entity_type ON carriers(entity_type);
CREATE INDEX IF NOT EXISTS idx_carriers_email ON carriers(email);
CREATE INDEX IF NOT EXISTS idx_carriers_safety_rating ON carriers(safety_rating);

-- Trigram indexes for fast ILIKE text search on carriers
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_carriers_legal_name_trgm ON carriers USING gin (legal_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_carriers_mc_number_trgm ON carriers USING gin (mc_number gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number_trgm ON carriers USING gin (dot_number gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_fmcsa_register_number ON fmcsa_register(number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_date_fetched ON fmcsa_register(date_fetched DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_category ON fmcsa_register(category);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

CREATE INDEX IF NOT EXISTS idx_blocked_ips_ip ON blocked_ips(ip_address);

CREATE INDEX IF NOT EXISTS idx_insurance_history_docket ON insurance_history(docket_number);
CREATE INDEX IF NOT EXISTS idx_insurance_history_docket_type ON insurance_history(docket_number, ins_type_desc);
CREATE INDEX IF NOT EXISTS idx_insurance_history_docket_cancl ON insurance_history(docket_number, cancl_effective_date);

-- ── Timestamp triggers ──────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_carriers_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_carriers_updated_at ON carriers;
CREATE TRIGGER update_carriers_updated_at BEFORE UPDATE ON carriers
    FOR EACH ROW EXECUTE FUNCTION update_carriers_updated_at();

-- VIP OPTIMIZATION: Auto-populate optimized columns for fast filtering
CREATE OR REPLACE FUNCTION carriers_auto_optimize()
RETURNS TRIGGER AS $$
BEGIN
    NEW.physical_state = extract_state(NEW.physical_address);
    NEW.power_units_int = text_to_int(NEW.power_units);
    NEW.drivers_int = text_to_int(NEW.drivers);
    NEW.crash_count = jsonb_array_length(COALESCE(NEW.crashes, '[]'::jsonb));
    NEW.inspection_count = jsonb_array_length(COALESCE(NEW.inspections, '[]'::jsonb));
    NEW.oos_violation_total = (
        SELECT COALESCE(SUM((elem->>'oosViolations')::int), 0)
        FROM jsonb_array_elements(COALESCE(NEW.inspections, '[]'::jsonb)) elem
    );
    NEW.injuries_total = (
        SELECT COALESCE(SUM(CASE WHEN elem->>'injuries' ~ '^[0-9]+$' THEN (elem->>'injuries')::int ELSE 0 END), 0)
        FROM jsonb_array_elements(COALESCE(NEW.crashes, '[]'::jsonb)) elem
    );
    NEW.fatalities_total = (
        SELECT COALESCE(COUNT(*), 0)
        FROM jsonb_array_elements(COALESCE(NEW.crashes, '[]'::jsonb)) elem
        WHERE elem->>'fatal' IS NOT NULL AND elem->>'fatal' NOT IN ('No', '0', '', 'N/A')
    );
    NEW.towaway_total = (
        SELECT COALESCE(COUNT(*), 0)
        FROM jsonb_array_elements(COALESCE(NEW.crashes, '[]'::jsonb)) elem
        WHERE elem->>'towaway' IS NOT NULL AND elem->>'towaway' NOT IN ('No', '0', '', 'N/A')
    );
    IF NEW.mcs150_date ~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN
        NEW.years_in_business = EXTRACT(YEAR FROM age(CURRENT_DATE, TO_DATE(NEW.mcs150_date, 'MM/DD/YYYY')));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS carriers_optimize_trigger ON carriers;
CREATE TRIGGER carriers_optimize_trigger BEFORE INSERT OR UPDATE ON carriers
    FOR EACH ROW EXECUTE FUNCTION carriers_auto_optimize();

CREATE OR REPLACE FUNCTION update_fmcsa_register_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_fmcsa_register_updated_at ON fmcsa_register;
CREATE TRIGGER update_fmcsa_register_updated_at BEFORE UPDATE ON fmcsa_register
    FOR EACH ROW EXECUTE FUNCTION update_fmcsa_register_updated_at();

CREATE OR REPLACE FUNCTION update_users_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_users_updated_at();

-- ── New Ventures table (ALL BrokerSnapshot CSV columns) ──────────────────────
CREATE TABLE IF NOT EXISTS new_ventures (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    dot_number TEXT,
    prefix TEXT,
    docket_number TEXT,
    status_code TEXT,
    carship TEXT,
    carrier_operation TEXT,
    name TEXT,
    name_dba TEXT,
    add_date TEXT,
    chgn_date TEXT,
    common_stat TEXT,
    contract_stat TEXT,
    broker_stat TEXT,
    common_app_pend TEXT,
    contract_app_pend TEXT,
    broker_app_pend TEXT,
    common_rev_pend TEXT,
    contract_rev_pend TEXT,
    broker_rev_pend TEXT,
    property_chk TEXT,
    passenger_chk TEXT,
    hhg_chk TEXT,
    private_auth_chk TEXT,
    enterprise_chk TEXT,
    operating_status TEXT,
    operating_status_indicator TEXT,
    phy_str TEXT,
    phy_city TEXT,
    phy_st TEXT,
    phy_zip TEXT,
    phy_country TEXT,
    phy_cnty TEXT,
    mai_str TEXT,
    mai_city TEXT,
    mai_st TEXT,
    mai_zip TEXT,
    mai_country TEXT,
    mai_cnty TEXT,
    phy_undeliv TEXT,
    mai_undeliv TEXT,
    phy_phone TEXT,
    phy_fax TEXT,
    mai_phone TEXT,
    mai_fax TEXT,
    cell_phone TEXT,
    email_address TEXT,
    company_officer_1 TEXT,
    company_officer_2 TEXT,
    genfreight TEXT,
    household TEXT,
    metalsheet TEXT,
    motorveh TEXT,
    drivetow TEXT,
    logpole TEXT,
    bldgmat TEXT,
    mobilehome TEXT,
    machlrg TEXT,
    produce TEXT,
    liqgas TEXT,
    intermodal TEXT,
    passengers TEXT,
    oilfield TEXT,
    livestock TEXT,
    grainfeed TEXT,
    coalcoke TEXT,
    meat TEXT,
    garbage TEXT,
    usmail TEXT,
    chem TEXT,
    drybulk TEXT,
    coldfood TEXT,
    beverages TEXT,
    paperprod TEXT,
    utility TEXT,
    farmsupp TEXT,
    construct TEXT,
    waterwell TEXT,
    cargoothr TEXT,
    cargoothr_desc TEXT,
    hm_ind TEXT,
    bipd_req TEXT,
    cargo_req TEXT,
    bond_req TEXT,
    bipd_file TEXT,
    cargo_file TEXT,
    bond_file TEXT,
    owntruck TEXT,
    owntract TEXT,
    owntrail TEXT,
    owncoach TEXT,
    ownschool_1_8 TEXT,
    ownschool_9_15 TEXT,
    ownschool_16 TEXT,
    ownbus_16 TEXT,
    ownvan_1_8 TEXT,
    ownvan_9_15 TEXT,
    ownlimo_1_8 TEXT,
    ownlimo_9_15 TEXT,
    ownlimo_16 TEXT,
    trmtruck TEXT,
    trmtract TEXT,
    trmtrail TEXT,
    trmcoach TEXT,
    trmschool_1_8 TEXT,
    trmschool_9_15 TEXT,
    trmschool_16 TEXT,
    trmbus_16 TEXT,
    trmvan_1_8 TEXT,
    trmvan_9_15 TEXT,
    trmlimo_1_8 TEXT,
    trmlimo_9_15 TEXT,
    trmlimo_16 TEXT,
    trptruck TEXT,
    trptract TEXT,
    trptrail TEXT,
    trpcoach TEXT,
    trpschool_1_8 TEXT,
    trpschool_9_15 TEXT,
    trpschool_16 TEXT,
    trpbus_16 TEXT,
    trpvan_1_8 TEXT,
    trpvan_9_15 TEXT,
    trplimo_1_8 TEXT,
    trplimo_9_15 TEXT,
    trplimo_16 TEXT,
    total_trucks TEXT,
    total_buses TEXT,
    total_pwr TEXT,
    fleetsize TEXT,
    inter_within_100 TEXT,
    inter_beyond_100 TEXT,
    total_inter_drivers TEXT,
    intra_within_100 TEXT,
    intra_beyond_100 TEXT,
    total_intra_drivers TEXT,
    total_drivers TEXT,
    avg_tld TEXT,
    total_cdl TEXT,
    review_type TEXT,
    review_id TEXT,
    review_date TEXT,
    recordable_crash_rate TEXT,
    mcs150_mileage TEXT,
    mcs151_mileage TEXT,
    mcs150_mileage_year TEXT,
    mcs150_date TEXT,
    safety_rating TEXT,
    safety_rating_date TEXT,
    arber TEXT,
    smartway TEXT,
    tia TEXT,
    tia_phone TEXT,
    tia_contact_name TEXT,
    tia_tool_free TEXT,
    tia_fax TEXT,
    tia_email TEXT,
    tia_website TEXT,
    phy_ups_store TEXT,
    mai_ups_store TEXT,
    phy_mail_box TEXT,
    mai_mail_box TEXT,
    raw_data JSONB,
    scrape_date TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(dot_number, add_date)
);

CREATE INDEX IF NOT EXISTS idx_new_ventures_dot_number ON new_ventures(dot_number);
CREATE INDEX IF NOT EXISTS idx_new_ventures_docket_number ON new_ventures(docket_number);
CREATE INDEX IF NOT EXISTS idx_new_ventures_add_date ON new_ventures(add_date);
CREATE INDEX IF NOT EXISTS idx_new_ventures_name ON new_ventures(name);
CREATE INDEX IF NOT EXISTS idx_new_ventures_phy_st ON new_ventures(phy_st);
CREATE INDEX IF NOT EXISTS idx_new_ventures_operating_status ON new_ventures(operating_status);
CREATE INDEX IF NOT EXISTS idx_new_ventures_created_at ON new_ventures(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_new_ventures_email ON new_ventures(email_address);
CREATE INDEX IF NOT EXISTS idx_new_ventures_hm_ind ON new_ventures(hm_ind);
CREATE INDEX IF NOT EXISTS idx_new_ventures_carrier_op ON new_ventures(carrier_operation);

-- Trigram indexes for fast ILIKE text search on new_ventures
CREATE INDEX IF NOT EXISTS idx_new_ventures_name_trgm ON new_ventures USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_name_dba_trgm ON new_ventures USING gin (name_dba gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_dot_trgm ON new_ventures USING gin (dot_number gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_docket_trgm ON new_ventures USING gin (docket_number gin_trgm_ops);

CREATE OR REPLACE FUNCTION update_new_ventures_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_new_ventures_updated_at ON new_ventures;
CREATE TRIGGER update_new_ventures_updated_at BEFORE UPDATE ON new_ventures
    FOR EACH ROW EXECUTE FUNCTION update_new_ventures_updated_at();

-- ── Default admin user ──────────────────────────────────────────────────────
-- Password is 'Admin123!'
INSERT INTO users (user_id, name, email, password_hash, role, plan, daily_limit)
VALUES ('1', 'Admin User', 'wooohan3@gmail.com', '$2b$12$00oFOpPbsszqmZDF3Q411O.2cPrwS/.GjPiNhJPiLpCEbE6Hlwy4C', 'admin', 'Enterprise', 100000)
ON CONFLICT (email) DO UPDATE SET password_hash = EXCLUDED.password_hash;
"""


async def connect_db() -> None:
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    try:
        async with _pool.acquire() as conn:
            # We separate the execution of standard columns and the rest to prevent failures cascading
            try:
                await conn.execute(_SCHEMA_SQL)
            except Exception as se:
                print(f"[DB] Partial schema init error: {se}")
            # Ensure optimized columns exist for existing databases
            await conn.execute("""
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS physical_state TEXT;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS power_units_int INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS drivers_int INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS crash_count INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS inspection_count INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS oos_violation_total INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS injuries_total INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS fatalities_total INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS towaway_total INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS years_in_business FLOAT DEFAULT 0;
            """)
        print("[DB] Connected to PostgreSQL, schema initialized, pool created")
    except Exception as e:
        print(f"[DB] Connected to PostgreSQL, pool created (schema init failed entirely: {e})")



async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
    _pool = None
    print("[DB] PostgreSQL connection pool closed")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database not connected. Call connect_db() first.")
    return _pool


async def upsert_carrier(record: dict) -> bool:
    pool = get_pool()
    mc = record.get("mc_number")
    if not mc:
        return False
    try:
        await pool.execute(
            """
            INSERT INTO carriers (
                mc_number, dot_number, legal_name, dba_name, entity_type,
                status, email, phone, power_units, drivers, non_cmv_units,
                physical_address, mailing_address, date_scraped,
                mcs150_date, mcs150_mileage, operation_classification,
                carrier_operation, cargo_carried, out_of_service_date,
                state_carrier_id, duns_number, safety_rating, safety_rating_date,
                basic_scores, oos_rates, insurance_policies, inspections, crashes
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10, $11,
                $12, $13, $14,
                $15, $16, $17,
                $18, $19, $20,
                $21, $22, $23, $24,
                $25, $26, $27, $28, $29
            )
            ON CONFLICT (mc_number) DO UPDATE SET
                dot_number = EXCLUDED.dot_number,
                legal_name = EXCLUDED.legal_name,
                dba_name = EXCLUDED.dba_name,
                entity_type = EXCLUDED.entity_type,
                status = EXCLUDED.status,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                power_units = EXCLUDED.power_units,
                drivers = EXCLUDED.drivers,
                non_cmv_units = EXCLUDED.non_cmv_units,
                physical_address = EXCLUDED.physical_address,
                mailing_address = EXCLUDED.mailing_address,
                date_scraped = EXCLUDED.date_scraped,
                mcs150_date = EXCLUDED.mcs150_date,
                mcs150_mileage = EXCLUDED.mcs150_mileage,
                operation_classification = EXCLUDED.operation_classification,
                carrier_operation = EXCLUDED.carrier_operation,
                cargo_carried = EXCLUDED.cargo_carried,
                out_of_service_date = EXCLUDED.out_of_service_date,
                state_carrier_id = EXCLUDED.state_carrier_id,
                duns_number = EXCLUDED.duns_number,
                safety_rating = EXCLUDED.safety_rating,
                safety_rating_date = EXCLUDED.safety_rating_date,
                basic_scores = EXCLUDED.basic_scores,
                oos_rates = EXCLUDED.oos_rates,
                insurance_policies = EXCLUDED.insurance_policies,
                inspections = EXCLUDED.inspections,
                crashes = EXCLUDED.crashes,
                updated_at = NOW()
            """,
            record.get("mc_number"),
            record.get("dot_number"),
            record.get("legal_name"),
            record.get("dba_name"),
            record.get("entity_type"),
            record.get("status"),
            record.get("email"),
            record.get("phone"),
            record.get("power_units"),
            record.get("drivers"),
            record.get("non_cmv_units"),
            record.get("physical_address"),
            record.get("mailing_address"),
            record.get("date_scraped"),
            record.get("mcs150_date"),
            record.get("mcs150_mileage"),
            record.get("operation_classification", []),
            record.get("carrier_operation", []),
            record.get("cargo_carried", []),
            record.get("out_of_service_date"),
            record.get("state_carrier_id"),
            record.get("duns_number"),
            record.get("safety_rating"),
            record.get("safety_rating_date"),
            _to_jsonb(record.get("basic_scores")),
            _to_jsonb(record.get("oos_rates")),
            _to_jsonb(record.get("insurance_policies")),
            _to_jsonb(record.get("inspections")),
            _to_jsonb(record.get("crashes")),
        )
        return True
    except Exception as e:
        print(f"[DB] Error upserting carrier {mc}: {e}")
        return False


async def update_carrier_insurance(dot_number: str, policies: list) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute(
            """
            UPDATE carriers
            SET insurance_policies = $1, updated_at = NOW()
            WHERE dot_number = $2
            """,
            _to_jsonb(policies),
            dot_number,
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error updating insurance for DOT {dot_number}: {e}")
        return False


async def save_fmcsa_register_entries(entries: list[dict], extracted_date: str) -> dict:
    pool = get_pool()
    if not entries:
        return {"success": True, "saved": 0, "skipped": 0}

    saved = 0
    skipped = 0
    batch_size = 500
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(entries), batch_size):
                    batch = entries[i:i + batch_size]
                    args = [
                        (
                            entry["number"],
                            entry.get("title", ""),
                            entry.get("decided", "N/A"),
                            entry.get("category", ""),
                            extracted_date,
                        )
                        for entry in batch
                    ]
                    await conn.executemany(
                        """
                        INSERT INTO fmcsa_register (number, title, decided, category, date_fetched)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (number, date_fetched) DO UPDATE SET
                            title = EXCLUDED.title,
                            decided = EXCLUDED.decided,
                            category = EXCLUDED.category,
                            updated_at = NOW()
                        """,
                        args,
                    )
                    saved += len(batch)
    except Exception as e:
        print(f"[DB] Error batch-saving FMCSA entries: {e}")
        skipped = len(entries) - saved

    return {"success": True, "saved": saved, "skipped": skipped}


async def fetch_fmcsa_register_by_date(
    extracted_date: str,
    category: Optional[str] = None,
    search_term: Optional[str] = None,
) -> list[dict]:
    pool = get_pool()

    conditions = ["date_fetched = $1"]
    params: list = [extracted_date]
    idx = 2

    if category:
        conditions.append(f"category = ${idx}")
        params.append(category)
        idx += 1

    if search_term:
        conditions.append(f"(title ILIKE ${idx} OR number ILIKE ${idx})")
        params.append(f"%{search_term}%")
        idx += 1

    where = " AND ".join(conditions)
    query = f"""
        SELECT number, title, decided, category, date_fetched
        FROM fmcsa_register
        WHERE {where}
        ORDER BY number
        LIMIT 10000
    """

    rows = await pool.fetch(query, *params)
    return [dict(row) for row in rows]


async def get_fmcsa_extracted_dates() -> list[str]:
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT DISTINCT date_fetched FROM fmcsa_register ORDER BY date_fetched DESC"
    )
    return [row["date_fetched"] for row in rows]


def _parse_jsonb(value) -> Optional[object]:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


def _format_insurance_history(raw_filings) -> list[dict]:
    if not raw_filings:
        return []
    results = []
    for row in raw_filings:
        raw_amount = (row.get("max_cov_amount") or "").strip()
        try:
            amount_int = int(raw_amount) * 1000
            coverage = f"${amount_int:,}"
        except (ValueError, TypeError):
            coverage = raw_amount or "N/A"
        cancl = (row.get("cancl_effective_date") or "").strip()
        results.append({
            "type": (row.get("ins_type_desc") or "").strip(),
            "coverageAmount": coverage,
            "policyNumber": (row.get("policy_no") or "").strip(),
            "effectiveDate": (row.get("effective_date") or "").strip(),
            "carrier": (row.get("name_company") or "").strip(),
            "formCode": (row.get("ins_form_code") or "").strip(),
            "transDate": (row.get("trans_date") or "").strip(),
            "underlLimAmount": (row.get("underl_lim_amount") or "").strip(),
            "canclEffectiveDate": cancl,
            "status": "Cancelled" if cancl else "Active",
        })
    return results


def _carrier_row_to_dict(row) -> dict:
    d = dict(row)
    # Map snake_case to camelCase for frontend
    mapping = {
        "mc_number": "mcNumber",
        "dot_number": "dotNumber",
        "legal_name": "legalName",
        "dba_name": "dbaName",
        "entity_type": "entityType",
        "power_units": "powerUnits",
        "non_cmv_units": "nonCmvUnits",
        "physical_address": "physicalAddress",
        "mailing_address": "mailingAddress",
        "date_scraped": "dateScraped",
        "mcs150_date": "mcs150Date",
        "mcs150_mileage": "mcs150Mileage",
        "operation_classification": "operationClassification",
        "carrier_operation": "carrierOperation",
        "cargo_carried": "cargoCarried",
        "out_of_service_date": "outOfServiceDate",
        "state_carrier_id": "stateCarrierId",
        "safety_rating": "safetyRating",
        "safety_rating_date": "safetyRatingDate",
        "basic_scores": "basicScores",
        "oos_rates": "oosRates",
        "insurance_policies": "insurancePolicies",
        "inspections": "inspections",
        "crashes": "crashes",
        "crash_count": "crashCount",
        "inspection_count": "inspectionCount",
        "oos_violation_total": "oosViolationTotal",
        "injuries_total": "injuriesTotal",
        "fatalities_total": "fatalitiesTotal",
        "towaway_total": "towawayTotal",
        "created_at": "createdAt",
        "updated_at": "updatedAt",
    }
    
    result = {}
    for k, v in d.items():
        new_key = mapping.get(k, k)
        result[new_key] = v
        
    for key in ("basicScores", "oosRates", "insurancePolicies", "inspections", "crashes"):
        if key in result:
            result[key] = _parse_jsonb(result[key])
            
    if "insurance_history_filings" in result:
        raw = _parse_jsonb(result["insurance_history_filings"])
        result["insuranceHistoryFilings"] = _format_insurance_history(raw)
        del result["insurance_history_filings"]
        
    if "inspections_list" in result:
        result["inspections"] = _parse_jsonb(result["inspections_list"])
        del result["inspections_list"]
        
    for key in ("createdAt", "updatedAt"):
        if key in result and result[key] is not None:
            if hasattr(result[key], "isoformat"):
                result[key] = result[key].isoformat()
                
    if "id" in result and result["id"] is not None:
        result["id"] = str(result["id"])
        
    # Standard mapping for common types
    if "entityType" in result and result["entityType"]:
        result["entityType"] = result["entityType"].upper()
        
    return result


async def fetch_carriers(filters: dict) -> dict:
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    if filters.get("mc_number"):
        conditions.append(f"c.mc_number ILIKE ${idx}")
        params.append(f"%{filters['mc_number']}%")
        idx += 1

    if filters.get("dot_number"):
        conditions.append(f"c.dot_number ILIKE ${idx}")
        params.append(f"%{filters['dot_number']}%")
        idx += 1

    if filters.get("legal_name"):
        # Use trigram index
        conditions.append(f"c.legal_name ILIKE ${idx}")
        params.append(f"%{filters['legal_name']}%")
        idx += 1

    entity_type = filters.get("entity_type")
    if entity_type:
        conditions.append(f"c.entity_type ILIKE ${idx}")
        params.append(f"%{entity_type}%")
        idx += 1

    active = filters.get("active")
    if active == "true":
        conditions.append(f"c.status ILIKE ${idx}")
        params.append("%AUTHORIZED%")
        idx += 1
        conditions.append(f"c.status NOT ILIKE ${idx}")
        params.append("%NOT%")
        idx += 1
    elif active == "false":
        conditions.append(f"(c.status ILIKE ${idx} OR c.status NOT ILIKE ${idx + 1})")
        params.append("%NOT AUTHORIZED%")
        params.append("%AUTHORIZED%")
        idx += 2

    if filters.get("years_in_business_min"):
        conditions.append(f"c.years_in_business >= ${idx}")
        params.append(float(filters["years_in_business_min"]))
        idx += 1
    if filters.get("years_in_business_max"):
        conditions.append(f"c.years_in_business <= ${idx}")
        params.append(float(filters["years_in_business_max"]))
        idx += 1

    if filters.get("state"):
        states = filters["state"].split("|")
        conditions.append(f"c.physical_state = ANY(${idx})")
        params.append(states)
        idx += 1

    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("c.email IS NOT NULL AND c.email != ''")
    elif has_email == "false":
        conditions.append("(c.email IS NULL OR c.email = '')")

    has_boc3 = filters.get("has_boc3")
    if has_boc3 == "true":
        conditions.append("carrier_operation @> ARRAY['BOC-3']")
    elif has_boc3 == "false":
        conditions.append("NOT (carrier_operation @> ARRAY['BOC-3'])")

    has_company_rep = filters.get("has_company_rep")
    if has_company_rep == "true":
        conditions.append("dba_name IS NOT NULL AND dba_name != ''")
    elif has_company_rep == "false":
        conditions.append("(dba_name IS NULL OR dba_name = '')")

    if filters.get("classification"):
        classifications = filters["classification"]
        if isinstance(classifications, str):
            classifications = classifications.split(",")
        conditions.append(f"operation_classification && ${idx}::text[]")
        params.append(classifications)
        idx += 1

    if filters.get("carrier_operation"):
        ops = filters["carrier_operation"]
        if isinstance(ops, str):
            ops = ops.split(",")
        conditions.append(f"carrier_operation && ${idx}::text[]")
        params.append(ops)
        idx += 1

    if filters.get("cargo"):
        cargo = filters["cargo"]
        if isinstance(cargo, str):
            cargo = cargo.split(",")
        conditions.append(f"cargo_carried && ${idx}::text[]")
        params.append(cargo)
        idx += 1

    hazmat = filters.get("hazmat")
    if hazmat == "true":
        conditions.append("cargo_carried @> ARRAY['Hazardous Materials']")
    elif hazmat == "false":
        conditions.append("NOT (cargo_carried @> ARRAY['Hazardous Materials'])")

    if filters.get("power_units_min"):
        conditions.append(f"c.power_units_int >= ${idx}")
        params.append(int(filters["power_units_min"]))
        idx += 1
    if filters.get("power_units_max"):
        conditions.append(f"c.power_units_int <= ${idx}")
        params.append(int(filters["power_units_max"]))
        idx += 1
    if filters.get("drivers_min"):
        conditions.append(f"c.drivers_int >= ${idx}")
        params.append(int(filters["drivers_min"]))
        idx += 1
    if filters.get("drivers_max"):
        conditions.append(f"c.drivers_int <= ${idx}")
        params.append(int(filters["drivers_max"]))
        idx += 1

    _INS_TYPE_PATTERN = {"BI&PD": "BIPD%", "CARGO": "CARGO", "BOND": "SURETY", "TRUST FUND": "TRUST FUND"}

    if filters.get("insurance_required"):
        ins_types = filters["insurance_required"]
        if isinstance(ins_types, str):
            ins_types = ins_types.split(",")
        or_clauses = []
        for itype in ins_types:
            pattern = _INS_TYPE_PATTERN.get(itype, itype)
            or_clauses.append(
                f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number AND ih.ins_type_desc LIKE ${idx} AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
            )
            params.append(pattern)
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    bipd_on_file = filters.get("bipd_on_file")
    if bipd_on_file == "1":
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number AND ih.ins_type_desc LIKE ${idx} AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("BIPD%")
        idx += 1
    elif bipd_on_file == "0":
        conditions.append(
            f"NOT EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number AND ih.ins_type_desc LIKE ${idx} AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("BIPD%")
        idx += 1
    cargo_on_file = filters.get("cargo_on_file")
    if cargo_on_file == "1":
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number AND ih.ins_type_desc = ${idx} AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("CARGO")
        idx += 1
    elif cargo_on_file == "0":
        conditions.append(
            f"NOT EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number AND ih.ins_type_desc = ${idx} AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("CARGO")
        idx += 1
    bond_on_file = filters.get("bond_on_file")
    if bond_on_file == "1":
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number AND ih.ins_type_desc = ${idx} AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("SURETY")
        idx += 1
    elif bond_on_file == "0":
        conditions.append(
            f"NOT EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number AND ih.ins_type_desc = ${idx} AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("SURETY")
        idx += 1
    trust_fund_on_file = filters.get("trust_fund_on_file")
    if trust_fund_on_file == "1":
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number AND ih.ins_type_desc = ${idx} AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("TRUST FUND")
        idx += 1
    elif trust_fund_on_file == "0":
        conditions.append(
            f"NOT EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number AND ih.ins_type_desc = ${idx} AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("TRUST FUND")
        idx += 1

    if filters.get("bipd_min"):
        raw_min = int(filters["bipd_min"])
        # max_cov_amount is stored in thousands (e.g. 750 = $750,000)
        # If the user typed a value >= 10000, assume they meant full dollars and convert to thousands
        compare_min = raw_min // 1000 if raw_min >= 10000 else raw_min
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number "
            f"AND NULLIF(REPLACE(ih.max_cov_amount, ',', ''), '')::numeric >= ${idx})"
        )
        params.append(compare_min)
        idx += 1
    if filters.get("bipd_max"):
        raw_max = int(filters["bipd_max"])
        # max_cov_amount is stored in thousands (e.g. 750 = $750,000)
        # If the user typed a value >= 10000, assume they meant full dollars and convert to thousands
        compare_max = raw_max // 1000 if raw_max >= 10000 else raw_max
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number "
            f"AND NULLIF(REPLACE(ih.max_cov_amount, ',', ''), '')::numeric <= ${idx})"
        )
        params.append(compare_max)
        idx += 1

    # effective_date is stored as MM/DD/YYYY (e.g. 05/18/2020)
    # Convert input from YYYY-MM-DD to MM/DD/YYYY so both sides match
    if filters.get("ins_effective_date_from"):
        # Convert 2026-03-01 -> 03/01/2026
        parts = filters["ins_effective_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number "
            f"AND ih.effective_date IS NOT NULL AND ih.effective_date LIKE '%/%/%' "
            f"AND TO_DATE(ih.effective_date, 'MM/DD/YYYY') >= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("ins_effective_date_to"):
        parts = filters["ins_effective_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number "
            f"AND ih.effective_date IS NOT NULL AND ih.effective_date LIKE '%/%/%' "
            f"AND TO_DATE(ih.effective_date, 'MM/DD/YYYY') <= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_to_db_fmt)
        idx += 1

    # cancl_effective_date is stored as MM/DD/YYYY (e.g. 05/31/2026)
    # Convert input from YYYY-MM-DD to MM/DD/YYYY so both sides match
    if filters.get("ins_cancellation_date_from"):
        parts = filters["ins_cancellation_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number "
            f"AND ih.cancl_effective_date IS NOT NULL AND ih.cancl_effective_date != '' AND ih.cancl_effective_date LIKE '%/%/%' "
            f"AND TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') >= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("ins_cancellation_date_to"):
        parts = filters["ins_cancellation_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number "
            f"AND ih.cancl_effective_date IS NOT NULL AND ih.cancl_effective_date != '' AND ih.cancl_effective_date LIKE '%/%/%' "
            f"AND TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') <= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_to_db_fmt)
        idx += 1

    if filters.get("oos_min"):
        conditions.append(f"c.oos_violation_total >= ${idx}")
        params.append(int(filters["oos_min"]))
        idx += 1
    if filters.get("oos_max"):
        conditions.append(f"c.oos_violation_total <= ${idx}")
        params.append(int(filters["oos_max"]))
        idx += 1

    if filters.get("crashes_min"):
        conditions.append(f"c.crash_count >= ${idx}")
        params.append(int(filters["crashes_min"]))
        idx += 1
    if filters.get("crashes_max"):
        conditions.append(f"c.crash_count <= ${idx}")
        params.append(int(filters["crashes_max"]))
        idx += 1

    if filters.get("injuries_min"):
        conditions.append(f"c.injuries_total >= ${idx}")
        params.append(int(filters["injuries_min"]))
        idx += 1
    if filters.get("injuries_max"):
        conditions.append(f"c.injuries_total <= ${idx}")
        params.append(int(filters["injuries_max"]))
        idx += 1

    if filters.get("fatalities_min"):
        conditions.append(f"c.fatalities_total >= ${idx}")
        params.append(int(filters["fatalities_min"]))
        idx += 1
    if filters.get("fatalities_max"):
        conditions.append(f"c.fatalities_total <= ${idx}")
        params.append(int(filters["fatalities_max"]))
        idx += 1

    if filters.get("inspections_min"):
        conditions.append(f"c.inspection_count >= ${idx}")
        params.append(int(filters["inspections_min"]))
        idx += 1
    if filters.get("inspections_max"):
        conditions.append(f"c.inspection_count <= ${idx}")
        params.append(int(filters["inspections_max"]))
        idx += 1

    if filters.get("toway_min"):
        conditions.append(f"c.towaway_total >= ${idx}")
        params.append(int(filters["toway_min"]))
        idx += 1
    if filters.get("toway_max"):
        conditions.append(f"c.towaway_total <= ${idx}")
        params.append(int(filters["toway_max"]))
        idx += 1

    # ── Insurance Company filter ──────────────────────────────────────────
    _INSURANCE_COMPANY_PATTERNS: dict[str, list[str]] = {
        "GREAT WEST CASUALTY": ["GREAT WEST%"],
        "UNITED FINANCIAL CASUALTY": ["UNITED FINANCIAL%"],
        "GEICO MARINE": ["GEICO MARINE%"],
        "NORTHLAND INSURANCE": ["NORTHLAND%"],
        "ARTISAN & TRUCKERS": ["ARTISAN%", "TRUCKERS CASUALTY%"],
        "CANAL INSURANCE": ["CANAL INS%"],
        "PROGRESSIVE": ["PROGRESSIVE%"],
        "BERKSHIRE HATHAWAY": ["BERKSHIRE%"],
        "OLD REPUBLIC": ["OLD REPUBLIC%"],
        "SENTRY": ["SENTRY%"],
        "TRAVELERS": ["TRAVELERS%"],
    }
    if filters.get("insurance_company"):
        companies = filters["insurance_company"]
        if isinstance(companies, str):
            companies = companies.split(",")
        or_clauses = []
        for company in companies:
            company_upper = company.strip().upper()
            patterns = _INSURANCE_COMPANY_PATTERNS.get(company_upper, [f"{company_upper}%"])
            for pattern in patterns:
                or_clauses.append(
                    f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number "
                    f"AND UPPER(ih.name_company) LIKE ${idx} "
                    f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '' "
                    f"OR TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') >= CURRENT_DATE))"
                )
                params.append(pattern)
                idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # ── Renewal Policy Monthly filter ─────────────────────────────────────
    # Renewal date = next anniversary of effective_date (annual renewal).
    # "1" means from today to end of next month, "2" means to end of month+2, etc.
    # Only active policies (cancl_effective_date is null/empty or >= today).
    if filters.get("renewal_policy_months"):
        months = int(filters["renewal_policy_months"])
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number "
            f"AND ih.effective_date IS NOT NULL AND ih.effective_date LIKE '%/%/%' "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '' "
            f"OR TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') >= CURRENT_DATE) "
            f"AND ("
            f"  CASE "
            f"    WHEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"         >= CURRENT_DATE "
            f"    THEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"    ELSE MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"  END"
            f") BETWEEN CURRENT_DATE AND (DATE_TRUNC('MONTH', CURRENT_DATE + MAKE_INTERVAL(months => ${idx})) + INTERVAL '1 MONTH - 1 DAY')::date"
            f")"
        )
        params.append(months)
        idx += 1

    # ── Renewal Policy Date range filter ──────────────────────────────────
    if filters.get("renewal_date_from"):
        parts = filters["renewal_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number "
            f"AND ih.effective_date IS NOT NULL AND ih.effective_date LIKE '%/%/%' "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '' "
            f"OR TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') >= CURRENT_DATE) "
            f"AND ("
            f"  CASE "
            f"    WHEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"         >= CURRENT_DATE "
            f"    THEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"    ELSE MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"  END"
            f") >= TO_DATE(${idx}, 'MM/DD/YYYY')"
            f")"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("renewal_date_to"):
        parts = filters["renewal_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = 'MC' || mc_number "
            f"AND ih.effective_date IS NOT NULL AND ih.effective_date LIKE '%/%/%' "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '' "
            f"OR TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') >= CURRENT_DATE) "
            f"AND ("
            f"  CASE "
            f"    WHEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"         >= CURRENT_DATE "
            f"    THEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"    ELSE MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"  END"
            f") <= TO_DATE(${idx}, 'MM/DD/YYYY')"
            f")"
        )
        params.append(date_to_db_fmt)
        idx += 1

    where = " AND ".join(conditions) if conditions else "TRUE"

    is_filtered = len(conditions) > 0
    if is_filtered:
        limit_val = int(filters.get("limit", 500))
    else:
        limit_val = int(filters.get("limit", 200))

    offset_val = int(filters.get("offset", 0))

    _LIGHT_COLS = """
        c.id, c.mc_number, c.dot_number, c.legal_name, c.dba_name,
        c.physical_address, c.physical_state, c.phone, c.email,
        c.status, c.entity_type, c.power_units, c.drivers,
        c.power_units_int, c.drivers_int,
        c.mcs150_date, c.mcs150_mileage, c.safety_rating, c.safety_rating_date,
        c.operation_classification, c.carrier_operation, c.cargo_carried,
        c.crash_count, c.inspection_count, c.oos_violation_total,
        c.injuries_total, c.fatalities_total, c.towaway_total,
        c.years_in_business, c.out_of_service_date, c.state_carrier_id,
        c.non_cmv_units, c.mailing_address, c.date_scraped, c.duns_number,
        c.basic_scores, c.oos_rates,
        c.created_at, c.updated_at
    """

    # Skip ORDER BY if there are filters to avoid massive memory sorts on 4M+ rows
    order_clause = "ORDER BY c.created_at DESC" if not is_filtered else "ORDER BY c.id DESC"

    query = f"""
        SELECT {_LIGHT_COLS}
        FROM carriers c
        WHERE {where}
        {order_clause}
        LIMIT {limit_val} OFFSET {offset_val}
    """

    try:
        if is_filtered:

            # For filtered: run data query + fast EXPLAIN estimate in parallel
            explain_query = f"EXPLAIN (FORMAT JSON) SELECT 1 FROM carriers c WHERE {where}"
            
            rows, explain_rows = await asyncio.gather(
                pool.fetch(query, *params),
                pool.fetch(explain_query, *params),
            )
            
            try:
                plan = json.loads(explain_rows[0][0])
                filtered_count = plan[0]['Plan']['Plan Rows']
                filtered_count = max(int(filtered_count), len(rows))
            except Exception:
                filtered_count = len(rows)
        else:
            # For unfiltered: use pg_class reltuples (instant)
            rows, count_row = await asyncio.gather(
                pool.fetch(query, *params),
                pool.fetchrow("SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'carriers'"),
            )
            filtered_count = count_row["cnt"] if count_row else 0
            
        return {
            "data": [_carrier_row_to_dict(row) for row in rows],
            "filtered_count": filtered_count,
        }
    except Exception as e:
        print(f"[DB] Error fetching carriers: {e}")
        return {"data": [], "filtered_count": 0}



async def delete_carrier(mc_number: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM carriers WHERE mc_number = $1", mc_number
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting carrier {mc_number}: {e}")
        return False


async def get_carrier_count() -> int:
    pool = get_pool()
    try:
        # Faster estimation for total count using pg_class
        row = await pool.fetchrow("SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'carriers'")
        if row and row["cnt"] > 0:
            return row["cnt"]
        # Fallback for small tables or if statistics are not yet gathered
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM carriers")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting carrier count: {e}")
        return 0


# In-memory cache for dashboard stats
_dashboard_stats_cache = {"data": None, "timestamp": 0}
_STATS_CACHE_TTL = 600  # 10 minutes

async def get_carrier_dashboard_stats() -> dict:
    import time
    global _dashboard_stats_cache
    
    # Return cached stats if valid
    now = time.time()
    if _dashboard_stats_cache["data"] and (now - _dashboard_stats_cache["timestamp"]) < _STATS_CACHE_TTL:
        return _dashboard_stats_cache["data"]

    pool = get_pool()
    try:
        # Run everything in parallel for maximum performance
        total_task = pool.fetchrow("SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'carriers'")
        inspections_task = pool.fetchrow("SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'inspections'")
        insurance_task = pool.fetchrow("SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'insurance_history'")
        
        # Parallel sub-counts
        active_task = pool.fetchval("SELECT COUNT(*) FROM carriers WHERE status ILIKE '%AUTHORIZED%' AND status NOT ILIKE '%NOT%'")
        broker_task = pool.fetchval("SELECT COUNT(*) FROM carriers WHERE entity_type ILIKE '%BROKER%'")
        email_task = pool.fetchval("SELECT COUNT(*) FROM carriers WHERE email IS NOT NULL AND email != ''")
        safety_task = pool.fetchval("SELECT COUNT(*) FROM carriers WHERE safety_rating IS NOT NULL AND safety_rating != ''")
        crash_task = pool.fetchval("SELECT COUNT(*) FROM carriers WHERE crash_count > 0")
        not_auth_task = pool.fetchval("SELECT COUNT(*) FROM carriers WHERE status ILIKE '%NOT AUTHORIZED%'")

        results = await asyncio.gather(
            total_task, inspections_task, insurance_task,
            active_task, broker_task, email_task, safety_task, crash_task, not_auth_task
        )
        
        total_row, insp_row, ins_row, active, brokers, with_email, with_safety, with_crashes, not_auth = results

        total = total_row["cnt"] if total_row and total_row["cnt"] > 0 else 0
        if total == 0: # Fallback
            total = await pool.fetchval("SELECT COUNT(*) FROM carriers")
            
        ins_cnt = ins_row["cnt"] if ins_row and ins_row["cnt"] > 0 else 0
        insp_cnt = insp_row["cnt"] if insp_row and insp_row["cnt"] > 0 else 0
        
        email_rate = f"{(with_email / total * 100):.1f}" if total > 0 else "0"
        
        data = {
            "total": total,
            "active_carriers": active or 0,
            "brokers": brokers or 0,
            "with_email": with_email or 0,
            "email_rate": email_rate,
            "with_safety_rating": with_safety or 0,
            "with_insurance": ins_cnt,
            "with_inspections": insp_cnt,
            "with_crashes": with_crashes or 0,
            "not_authorized": not_auth or 0,
            "other": max(0, total - (active or 0) - (not_auth or 0)),
        }
        
        # Update cache
        _dashboard_stats_cache = {"data": data, "timestamp": time.time()}
        return data
    except Exception as e:
        print(f"[DB] Error getting dashboard stats: {e}")
        # DON'T cache on error
        return {
            "total": 0, "active_carriers": 0, "brokers": 0,
            "with_email": 0, "email_rate": "0",
            "with_safety_rating": 0, "with_insurance": 0,
            "with_inspections": 0, "with_crashes": 0,
            "not_authorized": 0, "other": 0,
        }


async def update_carrier_safety(dot_number: str, safety_data: dict) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute(
            """
            UPDATE carriers
            SET safety_rating = $1,
                safety_rating_date = $2,
                basic_scores = $3,
                oos_rates = $4,
                updated_at = NOW()
            WHERE dot_number = $5
            """,
            safety_data.get("rating"),
            safety_data.get("ratingDate"),
            _to_jsonb(safety_data.get("basicScores")),
            _to_jsonb(safety_data.get("oosRates")),
            dot_number,
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error updating safety for DOT {dot_number}: {e}")
        return False


async def get_carriers_by_mc_range(start: str, end: str) -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            """
            SELECT * FROM carriers
            WHERE mc_number ~ '^[0-9]+$'
              AND mc_number::bigint >= $1::bigint
              AND mc_number::bigint <= $2::bigint
            ORDER BY mc_number::bigint ASC
            """,
            start,
            end,
        )
        return [_carrier_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching MC range: {e}")
        return []


def _user_row_to_dict(row) -> dict:
    d = dict(row)
    d.pop("password_hash", None)
    for key in ("created_at", "updated_at", "blocked_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])
    return d


async def fetch_users() -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT id, user_id, name, email, role, plan, daily_limit, "
            "records_extracted_today, last_active, ip_address, is_online, "
            "is_blocked, created_at, updated_at FROM users ORDER BY created_at DESC"
        )
        return [_user_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching users: {e}")
        return []


async def fetch_user_by_email(email: str) -> Optional[dict]:
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT id, user_id, name, email, role, plan, daily_limit, "
            "records_extracted_today, last_active, ip_address, is_online, "
            "is_blocked, created_at, updated_at FROM users WHERE email = $1",
            email.lower(),
        )
        if row:
            return _user_row_to_dict(row)
        return None
    except Exception as e:
        print(f"[DB] Error fetching user by email: {e}")
        return None


async def create_user(user_data: dict) -> Optional[dict]:
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            """
            INSERT INTO users (user_id, name, email, password_hash, role, plan,
                               daily_limit, records_extracted_today, last_active,
                               ip_address, is_online, is_blocked)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING *
            """,
            user_data.get("user_id"),
            user_data.get("name"),
            user_data.get("email", "").lower(),
            user_data.get("password_hash"),
            user_data.get("role", "user"),
            user_data.get("plan", "Free"),
            user_data.get("daily_limit", 50),
            user_data.get("records_extracted_today", 0),
            user_data.get("last_active", "Never"),
            user_data.get("ip_address"),
            user_data.get("is_online", False),
            user_data.get("is_blocked", False),
        )
        if row:
            return _user_row_to_dict(row)
        return None
    except Exception as e:
        print(f"[DB] Error creating user: {e}")
        return None


async def update_user(user_id: str, user_data: dict) -> bool:
    pool = get_pool()
    _ALLOWED_COLUMNS = {
        "name", "role", "plan", "daily_limit",
        "records_extracted_today", "last_active",
        "ip_address", "is_online", "is_blocked",
    }
    columns = {k: v for k, v in user_data.items() if k in _ALLOWED_COLUMNS}
    if not columns:
        return False
    set_clauses = []
    values = []
    for idx, (col, val) in enumerate(columns.items(), start=1):
        set_clauses.append(f"{col} = ${idx}")
        values.append(val)
    values.append(user_id)
    query = f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = ${len(values)}"
    try:
        result = await pool.execute(query, *values)
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error updating user {user_id}: {e}")
        return False


async def delete_user(user_id: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM users WHERE user_id = $1", user_id
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting user {user_id}: {e}")
        return False


async def get_user_password_hash(email: str) -> Optional[str]:
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT password_hash FROM users WHERE email = $1", email.lower()
        )
        if row and row["password_hash"]:
            return row["password_hash"]
        return None
    except Exception as e:
        print(f"[DB] Error fetching password hash: {e}")
        return None


async def fetch_blocked_ips() -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT * FROM blocked_ips ORDER BY blocked_at DESC"
        )
        return [_user_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching blocked IPs: {e}")
        return []


async def block_ip(ip_address: str, reason: str) -> bool:
    pool = get_pool()
    try:
        await pool.execute(
            """
            INSERT INTO blocked_ips (ip_address, reason)
            VALUES ($1, $2)
            ON CONFLICT (ip_address) DO NOTHING
            """,
            ip_address,
            reason or "No reason provided",
        )
        return True
    except Exception as e:
        print(f"[DB] Error blocking IP {ip_address}: {e}")
        return False


async def unblock_ip(ip_address: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM blocked_ips WHERE ip_address = $1", ip_address
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error unblocking IP {ip_address}: {e}")
        return False


async def is_ip_blocked(ip_address: str) -> bool:
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT ip_address FROM blocked_ips WHERE ip_address = $1",
            ip_address,
        )
        return row is not None
    except Exception as e:
        print(f"[DB] Error checking IP block status: {e}")
        return False


async def get_fmcsa_categories() -> list[str]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT DISTINCT category FROM fmcsa_register WHERE category IS NOT NULL ORDER BY category"
        )
        return [row["category"] for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching FMCSA categories: {e}")
        return []


async def delete_fmcsa_entries_before_date(date: str) -> int:
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM fmcsa_register WHERE date_fetched < $1", date
        )
        parts = result.split(" ")
        return int(parts[-1]) if len(parts) > 1 else 0
    except Exception as e:
        print(f"[DB] Error deleting FMCSA entries: {e}")
        return 0


def _to_jsonb(value) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value)


_NV_COLUMNS = [
    "dot_number", "prefix", "docket_number", "status_code", "carship",
    "carrier_operation", "name", "name_dba", "add_date", "chgn_date",
    "common_stat", "contract_stat", "broker_stat",
    "common_app_pend", "contract_app_pend", "broker_app_pend",
    "common_rev_pend", "contract_rev_pend", "broker_rev_pend",
    "property_chk", "passenger_chk", "hhg_chk", "private_auth_chk", "enterprise_chk",
    "operating_status", "operating_status_indicator",
    "phy_str", "phy_city", "phy_st", "phy_zip", "phy_country", "phy_cnty",
    "mai_str", "mai_city", "mai_st", "mai_zip", "mai_country", "mai_cnty",
    "phy_undeliv", "mai_undeliv",
    "phy_phone", "phy_fax", "mai_phone", "mai_fax", "cell_phone", "email_address",
    "company_officer_1", "company_officer_2",
    "genfreight", "household", "metalsheet", "motorveh", "drivetow", "logpole",
    "bldgmat", "mobilehome", "machlrg", "produce", "liqgas", "intermodal",
    "passengers", "oilfield", "livestock", "grainfeed", "coalcoke", "meat",
    "garbage", "usmail", "chem", "drybulk", "coldfood", "beverages",
    "paperprod", "utility", "farmsupp", "construct", "waterwell",
    "cargoothr", "cargoothr_desc",
    "hm_ind", "bipd_req", "cargo_req", "bond_req", "bipd_file", "cargo_file", "bond_file",
    "owntruck", "owntract", "owntrail", "owncoach",
    "ownschool_1_8", "ownschool_9_15", "ownschool_16", "ownbus_16",
    "ownvan_1_8", "ownvan_9_15", "ownlimo_1_8", "ownlimo_9_15", "ownlimo_16",
    "trmtruck", "trmtract", "trmtrail", "trmcoach",
    "trmschool_1_8", "trmschool_9_15", "trmschool_16", "trmbus_16",
    "trmvan_1_8", "trmvan_9_15", "trmlimo_1_8", "trmlimo_9_15", "trmlimo_16",
    "trptruck", "trptract", "trptrail", "trpcoach",
    "trpschool_1_8", "trpschool_9_15", "trpschool_16", "trpbus_16",
    "trpvan_1_8", "trpvan_9_15", "trplimo_1_8", "trplimo_9_15", "trplimo_16",
    "total_trucks", "total_buses", "total_pwr", "fleetsize",
    "inter_within_100", "inter_beyond_100", "total_inter_drivers",
    "intra_within_100", "intra_beyond_100", "total_intra_drivers",
    "total_drivers", "avg_tld", "total_cdl",
    "review_type", "review_id", "review_date", "recordable_crash_rate",
    "mcs150_mileage", "mcs151_mileage", "mcs150_mileage_year", "mcs150_date",
    "safety_rating", "safety_rating_date",
    "arber", "smartway", "tia", "tia_phone", "tia_contact_name",
    "tia_tool_free", "tia_fax", "tia_email", "tia_website",
    "phy_ups_store", "mai_ups_store", "phy_mail_box", "mai_mail_box",
]


def _new_venture_row_to_dict(row) -> dict:
    d = dict(row)
    if "raw_data" in d:
        d["raw_data"] = _parse_jsonb(d["raw_data"])
    for key in ("created_at", "updated_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])
    return d


async def save_new_venture_entries(entries: list[dict], scrape_date: str) -> dict:
    pool = get_pool()
    if not entries:
        return {"success": True, "saved": 0, "skipped": 0}

    cols = _NV_COLUMNS + ["raw_data", "scrape_date"]
    col_list = ", ".join(cols)
    placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))

    update_cols = [c for c in cols if c not in ("dot_number", "add_date")]
    on_conflict_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    on_conflict_set += ", updated_at = NOW()"

    insert_sql = f"""
        INSERT INTO new_ventures ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (dot_number, add_date) DO UPDATE SET {on_conflict_set}
    """

    saved = 0
    skipped = 0
    batch_size = 500

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(entries), batch_size):
                    batch = entries[i:i + batch_size]
                    args = []
                    for entry in batch:
                        row_args = []
                        for col in _NV_COLUMNS:
                            val = entry.get(col)
                            row_args.append(val.strip() if isinstance(val, str) else val)
                        row_args.append(_to_jsonb(entry.get("raw_data")))
                        row_args.append(scrape_date)
                        args.append(tuple(row_args))
                    await conn.executemany(insert_sql, args)
                    saved += len(batch)
    except Exception as e:
        print(f"[DB] Error batch-saving new venture entries: {e}")
        skipped = len(entries) - saved

    return {"success": True, "saved": saved, "skipped": skipped}


async def fetch_new_ventures(filters: dict) -> list[dict]:
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    if filters.get("docket_number"):
        conditions.append(f"docket_number ILIKE ${idx}")
        params.append(f"%{filters['docket_number']}%")
        idx += 1

    if filters.get("dot_number"):
        conditions.append(f"dot_number ILIKE ${idx}")
        params.append(f"%{filters['dot_number']}%")
        idx += 1

    if filters.get("company_name"):
        conditions.append(f"(name ILIKE ${idx} OR name_dba ILIKE ${idx})")
        params.append(f"%{filters['company_name']}%")
        idx += 1

    if filters.get("date_from"):
        conditions.append(f"add_date >= ${idx}")
        params.append(filters["date_from"])
        idx += 1
    if filters.get("date_to"):
        conditions.append(f"add_date <= ${idx}")
        params.append(filters["date_to"])
        idx += 1

    active = filters.get("active")
    if active == "active":
        conditions.append(f"((operating_status ILIKE ${idx} AND operating_status NOT ILIKE ${idx + 1}) OR operating_status ILIKE ${idx + 2})")
        params.append("%AUTHORIZED%")
        params.append("%NOT AUTHORIZED%")
        params.append("ACTIVE")
        idx += 3
    elif active == "inactive":
        conditions.append(f"(operating_status ILIKE ${idx} OR operating_status IS NULL OR operating_status = '')")
        params.append("%NOT AUTHORIZED%")
        idx += 1
    elif active == "authorization_pending":
        conditions.append(f"operating_status ILIKE ${idx}")
        params.append("%PENDING%")
        idx += 1
    elif active == "not_authorized":
        conditions.append(f"operating_status ILIKE ${idx}")
        params.append("%NOT AUTHORIZED%")
        idx += 1
    elif active == "true":
        conditions.append(f"((operating_status ILIKE ${idx} AND operating_status NOT ILIKE ${idx + 1}) OR operating_status ILIKE ${idx + 2})")
        params.append("%AUTHORIZED%")
        params.append("%NOT AUTHORIZED%")
        params.append("ACTIVE")
        idx += 3
    elif active == "false":
        conditions.append(f"operating_status NOT ILIKE ${idx}")
        params.append("%AUTHORIZED%")
        idx += 1

    if filters.get("state"):
        states = [s.strip().upper() for s in filters["state"].split("|") if s.strip()]
        if len(states) == 1:
            conditions.append(f"phy_st = ${idx}")
            params.append(states[0])
            idx += 1
        elif states:
            placeholders = ", ".join(f"${idx + i}" for i in range(len(states)))
            conditions.append(f"phy_st IN ({placeholders})")
            for s in states:
                params.append(s)
                idx += 1

    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("email_address IS NOT NULL AND email_address != ''")
    elif has_email == "false":
        conditions.append("(email_address IS NULL OR email_address = '')")

    if filters.get("carrier_operation"):
        ops = [o.strip() for o in filters["carrier_operation"].split("|") if o.strip()]
        if not ops:
            ops = [o.strip() for o in filters["carrier_operation"].split(",") if o.strip()]
        
        if ops:
            conditions.append(f"carrier_operation && ${idx}::text[]")
            params.append(ops)
            idx += 1

    if filters.get("hazmat"):
        if filters["hazmat"] == "true":
            conditions.append("hm_ind = 'Y'")
        elif filters["hazmat"] == "false":
            conditions.append("(hm_ind IS NULL OR hm_ind != 'Y')")

    if filters.get("power_units_min"):
        conditions.append(f"NULLIF(total_pwr, '')::int >= ${idx}")
        params.append(int(filters["power_units_min"]))
        idx += 1
    if filters.get("power_units_max"):
        conditions.append(f"NULLIF(total_pwr, '')::int <= ${idx}")
        params.append(int(filters["power_units_max"]))
        idx += 1

    if filters.get("drivers_min"):
        conditions.append(f"NULLIF(total_drivers, '')::int >= ${idx}")
        params.append(int(filters["drivers_min"]))
        idx += 1
    if filters.get("drivers_max"):
        conditions.append(f"NULLIF(total_drivers, '')::int <= ${idx}")
        params.append(int(filters["drivers_max"]))
        idx += 1

    if filters.get("bipd_on_file"):
        if filters["bipd_on_file"] == "true":
            conditions.append("bipd_file = 'Y'")
        elif filters["bipd_on_file"] == "false":
            conditions.append("(bipd_file IS NULL OR bipd_file != 'Y')")
    if filters.get("cargo_on_file"):
        if filters["cargo_on_file"] == "true":
            conditions.append("cargo_file = 'Y'")
        elif filters["cargo_on_file"] == "false":
            conditions.append("(cargo_file IS NULL OR cargo_file != 'Y')")
    if filters.get("bond_on_file"):
        if filters["bond_on_file"] == "true":
            conditions.append("bond_file = 'Y'")
        elif filters["bond_on_file"] == "false":
            conditions.append("(bond_file IS NULL OR bond_file != 'Y')")

    where = " AND ".join(conditions) if conditions else "TRUE"

    is_filtered = len(conditions) > 0
    if is_filtered:
        limit_val = int(filters.get("limit", 10000))
    else:
        limit_val = int(filters.get("limit", 200))

    offset_val = int(filters.get("offset", 0))

    query = f"""
        SELECT * FROM new_ventures
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT {limit_val} OFFSET {offset_val}
    """

    count_query = f"""
        SELECT COUNT(*) as cnt FROM new_ventures
        WHERE {where}
    """

    dates_query = "SELECT DISTINCT add_date FROM new_ventures WHERE add_date IS NOT NULL ORDER BY add_date DESC"

    total_query = "SELECT COUNT(*) as cnt FROM new_ventures"

    try:
        rows = await pool.fetch(query, *params)
        count_row = await pool.fetchrow(count_query, *params)
        filtered_count = count_row["cnt"] if count_row else 0
        date_rows = await pool.fetch(dates_query)
        available_dates = [r["add_date"] for r in date_rows]
        total_row = await pool.fetchrow(total_query)
        total_count = total_row["cnt"] if total_row else 0
        return {
            "data": [_new_venture_row_to_dict(row) for row in rows],
            "filtered_count": filtered_count,
            "total_count": total_count,
            "available_dates": available_dates,
        }
    except Exception as e:
        print(f"[DB] Error fetching new ventures: {e}")
        return {"data": [], "filtered_count": 0, "total_count": 0, "available_dates": []}


async def get_new_venture_count() -> int:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM new_ventures")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting new venture count: {e}")
        return 0


async def get_new_venture_scraped_dates() -> list[str]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT DISTINCT add_date FROM new_ventures WHERE add_date IS NOT NULL ORDER BY add_date DESC"
        )
        return [row["add_date"] for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching new venture dates: {e}")
        return []


async def fetch_new_venture_by_id(record_id: str) -> Optional[dict]:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT * FROM new_ventures WHERE id = $1", record_id)
        if row:
            return _new_venture_row_to_dict(row)
        return None
    except Exception as e:
        print(f"[DB] Error fetching new venture {record_id}: {e}")
        return None


async def delete_new_venture(record_id: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM new_ventures WHERE id = $1", record_id
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting new venture {record_id}: {e}")
        return False


async def fetch_insurance_history(mc_number: str) -> list[dict]:
    pool = get_pool()
    docket = f"MC{mc_number}"
    try:
        rows = await pool.fetch(
            """
            SELECT docket_number, dot_number, ins_form_code, ins_type_desc,
                   name_company, policy_no, trans_date, underl_lim_amount,
                   max_cov_amount, effective_date, cancl_effective_date
            FROM insurance_history
            WHERE docket_number = $1
            ORDER BY effective_date DESC
            """,
            docket,
        )
        results = []
        for row in rows:
            raw_amount = (row["max_cov_amount"] or "").strip()
            try:
                amount_int = int(raw_amount) * 1000
                coverage = f"${amount_int:,}"
            except (ValueError, TypeError):
                coverage = raw_amount or "N/A"
            cancl = (row["cancl_effective_date"] or "").strip()
            results.append({
                "type": (row["ins_type_desc"] or "").strip(),
                "coverageAmount": coverage,
                "policyNumber": (row["policy_no"] or "").strip(),
                "effectiveDate": (row["effective_date"] or "").strip(),
                "carrier": (row["name_company"] or "").strip(),
                "formCode": (row["ins_form_code"] or "").strip(),
                "transDate": (row["trans_date"] or "").strip(),
                "underlLimAmount": (row["underl_lim_amount"] or "").strip(),
                "canclEffectiveDate": cancl,
                "status": "Cancelled" if cancl else "Active",
            })
        return results
    except Exception as e:
        print(f"[DB] Error fetching insurance history for MC {mc_number}: {e}")
        return []

async def fetch_inspections(dot_number: str) -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            """
            SELECT unique_id, report_number, report_state, dot_number, insp_date,
                   insp_level_id, county_code_state, time_weight, driver_oos_total,
                   vehicle_oos_total, total_hazmat_sent, oos_total, hazmat_oos_total,
                   hazmat_placard_req, unit_type_desc, unit_make, unit_license,
                   unit_license_state, vin, unit_decal_number, unit_type_desc2,
                   unit_make2, unit_license2, unit_license_state2, vin2,
                   unit_decal_number2, unsafe_insp, fatigued_insp, dr_fitness_insp,
                   subt_alcohol_insp, vh_maint_insp, hm_insp, basic_viol,
                   unsafe_viol, fatigued_viol, dr_fitness_viol, subt_alcohol_viol,
                   vh_maint_viol, hm_viol
            FROM inspections
            WHERE dot_number = $1
            ORDER BY insp_date DESC
            """,
            dot_number,
        )
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching inspections for DOT {dot_number}: {e}")
        return []
