import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

async def update_trigger():
    if not DATABASE_URL:
        print("DATABASE_URL not set")
        return

    conn = await asyncpg.connect(DATABASE_URL)
    print("Connected to DB...")

    trigger_f_sql = """
CREATE OR REPLACE FUNCTION carriers_auto_optimize()
RETURNS TRIGGER AS $$
BEGIN
    NEW.physical_state = extract_state(NEW.physical_address);
    NEW.power_units_int = text_to_int(NEW.power_units);
    NEW.drivers_int = text_to_int(NEW.drivers);
    
    -- Only auto-calculate if JSONB data changed or on new insert
    -- This allows manual overrides to stay unless new data is scraped/imported
    
    IF TG_OP = 'INSERT' OR NEW.crashes IS DISTINCT FROM OLD.crashes THEN
        NEW.crash_count = jsonb_array_length(COALESCE(NEW.crashes, '[]'::jsonb));
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
    END IF;

    IF TG_OP = 'INSERT' OR NEW.inspections IS DISTINCT FROM OLD.inspections THEN
        NEW.inspection_count = jsonb_array_length(COALESCE(NEW.inspections, '[]'::jsonb));
        NEW.oos_violation_total = (
            SELECT COALESCE(SUM((elem->>'oosViolations')::int), 0)
            FROM jsonb_array_elements(COALESCE(NEW.inspections, '[]'::jsonb)) elem
        );
    END IF;

    IF TG_OP = 'INSERT' OR NEW.mcs150_date IS DISTINCT FROM OLD.mcs150_date THEN
        IF NEW.mcs150_date ~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN
            NEW.years_in_business = EXTRACT(YEAR FROM age(CURRENT_DATE, TO_DATE(NEW.mcs150_date, 'MM/DD/YYYY')));
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
    """

    try:
        await conn.execute(trigger_f_sql)
        print("Trigger function updated successfully!")
    except Exception as e:
        print(f"Error updating trigger: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(update_trigger())
