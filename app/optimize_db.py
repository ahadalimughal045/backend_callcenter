import os
import asyncio
import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

async def optimize():
    if not DATABASE_URL:
        print("DATABASE_URL not set")
        return

    pool = await asyncpg.create_pool(DATABASE_URL)
    print("Connected to DB for optimization...")

    try:
        async with pool.acquire() as conn:
            # 1. Add columns if they don't exist
            print("Adding optimized columns...")
            await conn.execute("""
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS physical_state TEXT;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS power_units_int INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS drivers_int INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS crash_count INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS inspection_count INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS oos_violation_total INTEGER DEFAULT 0;
                ALTER TABLE carriers ADD COLUMN IF NOT EXISTS years_in_business FLOAT DEFAULT 0;
            """)

            # 2. Add Trigram extension if not exists
            await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

            # 3. Create helper functions for extraction
            print("Updating helper functions...")
            await conn.execute("""
                CREATE OR REPLACE FUNCTION extract_state(addr TEXT) RETURNS TEXT AS $$
                DECLARE
                    parts TEXT[];
                BEGIN
                    IF addr IS NULL THEN RETURN NULL; END IF;
                    parts := string_to_array(addr, ',');
                    IF array_length(parts, 1) < 2 THEN RETURN NULL; END IF;
                    RETURN trim(split_part(trim(parts[array_length(parts, 1)]), ' ', 1));
                EXCEPTION WHEN OTHERS THEN
                    RETURN NULL;
                END;
                $$ LANGUAGE plpgsql IMMUTABLE;

                CREATE OR REPLACE FUNCTION text_to_int(t TEXT) RETURNS INTEGER AS $$
                BEGIN
                    IF t ~ '^[0-9]+$' THEN
                        RETURN t::integer;
                    END IF;
                    RETURN 0;
                EXCEPTION WHEN OTHERS THEN
                    RETURN 0;
                END;
                $$ LANGUAGE plpgsql IMMUTABLE;
            """)

        # 4. Populate columns in batches
        print("Populating columns in batches...")
        batch_size = 50000
        while True:
            async with pool.acquire() as conn:
                # Find IDs that need update
                rows = await conn.fetch("""
                    SELECT id FROM carriers 
                    WHERE physical_state IS NULL 
                    LIMIT $1
                """, batch_size)
                
                if not rows:
                    break
                    
                ids = [r['id'] for r in rows]
                print(f"Updating batch of {len(ids)} rows...")
                
                await conn.execute("""
                    UPDATE carriers 
                    SET 
                        physical_state = extract_state(physical_address),
                        power_units_int = text_to_int(power_units),
                        drivers_int = text_to_int(drivers),
                        crash_count = jsonb_array_length(COALESCE(crashes, '[]'::jsonb)),
                        inspection_count = jsonb_array_length(COALESCE(inspections, '[]'::jsonb)),
                        oos_violation_total = (
                            SELECT COALESCE(SUM((elem->>'oosViolations')::int), 0)
                            FROM jsonb_array_elements(COALESCE(inspections, '[]'::jsonb)) elem
                        ),
                        years_in_business = CASE 
                            WHEN mcs150_date ~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' 
                            THEN EXTRACT(YEAR FROM age(CURRENT_DATE, TO_DATE(mcs150_date, 'MM/DD/YYYY')))
                            ELSE 0 
                        END,
                        -- VIP: Denormalize aggregated insurance
                        insurance_policies = (
                            SELECT jsonb_agg(ih ORDER BY ih.effective_date DESC)
                            FROM insurance_history ih
                            WHERE ih.docket_number = 'MC' || carriers.mc_number
                        ),
                        -- VIP: Ensure audits/inspections are synced if needed
                         updated_at = NOW()
                    WHERE id = ANY($1)
                """, ids)
                
            # VIP: Sleep 1 second after each batch to let other queries run
            print("Batch finished. Sleeping 1s...")
            await asyncio.sleep(1)

        async with pool.acquire() as conn:
            # 5. Create Indexes
            print("Creating indexes...")
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_carriers_physical_state ON carriers(physical_state);
                CREATE INDEX IF NOT EXISTS idx_carriers_power_units_int ON carriers(power_units_int);
                CREATE INDEX IF NOT EXISTS idx_carriers_drivers_int ON carriers(drivers_int);
                CREATE INDEX IF NOT EXISTS idx_carriers_crash_count ON carriers(crash_count);
                CREATE INDEX IF NOT EXISTS idx_carriers_inspection_count ON carriers(inspection_count);
                CREATE INDEX IF NOT EXISTS idx_carriers_oos_violation_total ON carriers(oos_violation_total);
                CREATE INDEX IF NOT EXISTS idx_carriers_years_in_business ON carriers(years_in_business);
                CREATE INDEX IF NOT EXISTS idx_carriers_dba_name_trgm ON carriers USING gin (dba_name gin_trgm_ops);
                CREATE INDEX IF NOT EXISTS idx_carriers_operation_class_gin ON carriers USING gin (operation_classification);
                CREATE INDEX IF NOT EXISTS idx_carriers_carrier_op_gin ON carriers USING gin (carrier_operation);
                CREATE INDEX IF NOT EXISTS idx_carriers_cargo_carried_gin ON carriers USING gin (cargo_carried);
            """)

        print("Optimization complete!")

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(optimize())
