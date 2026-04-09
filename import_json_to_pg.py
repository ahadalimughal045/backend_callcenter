import asyncio
import json
import os
import glob
import time
from dotenv import load_dotenv
import asyncpg
from datetime import datetime

# Configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# JSON files are in the parent directory of 'backed/'
JSON_DIR = os.path.dirname(SCRIPT_DIR)
BATCH_SIZE = 10000 # Increased batch size for better performance

async def import_from_json():
    # Load environment variables from backed/.env
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        print("❌ DATABASE_URL not set in .env")
        return

    # Find all partial json files
    json_files = glob.glob(os.path.join(JSON_DIR, "carriers_data_partial_*.json"))
    print(f"🚀 Found {len(json_files)} JSON files to process.")

    if not json_files:
        print("❌ No JSON files found. Check JSON_DIR path.")
        return

    conn = await asyncpg.connect(db_url)
    try:
        # Get existing column names for verification
        columns = await conn.fetch("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'carriers'
        """)
        col_names = [c['column_name'] for c in columns]
        print(f"📊 Database columns found: {col_names}")

        total_inserted = 0
        start_time = time.time()

        for file_path in json_files:
            print(f"📦 Processing {os.path.basename(file_path)}...")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                carriers = data.get('carriers', [])
                if not carriers:
                    continue

                # Batch insert into carriers table with conflict handling
                # Using executemany with unnest or a manual multi-row insert
                # Conventional multi-row insert is faster in asyncpg for medium batches
                
                query = """
                    INSERT INTO carriers (
                        mc_number, dot_number, legal_name, dba_name, entity_type, status,
                        email, phone, power_units, drivers, non_cmv_units,
                        physical_address, mailing_address, date_scraped, mcs150_date, mcs150_mileage,
                        carrier_operation, cargo_carried,
                        state_carrier_id, duns_number,
                        safety_rating, safety_rating_date
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                        $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
                    )
                    ON CONFLICT (mc_number) DO UPDATE SET
                        dot_number = EXCLUDED.dot_number,
                        legal_name = EXCLUDED.legal_name,
                        dba_name = EXCLUDED.dba_name,
                        status = EXCLUDED.status,
                        email = EXCLUDED.email,
                        phone = EXCLUDED.phone,
                        power_units = EXCLUDED.power_units,
                        drivers = EXCLUDED.drivers,
                        updated_at = NOW()
                """
                
                records_batch = []
                count_in_file = 0
                for c in carriers:
                    mc = c.get('mc_number')
                    dot = str(c.get('dot_number', "0"))
                    
                    if mc is None or mc == "null" or mc == "NaN":
                        mc = f"DOT_{dot}"
                    
                    records_batch.append((
                        str(mc),
                        dot,
                        str(c.get('legal_name') or 'Unknown'),
                        c.get('dba_name'),
                        c.get('entity_type'),
                        c.get('status'),
                        c.get('email'),
                        c.get('phone'),
                        str(c.get('power_units', '0')),
                        str(c.get('drivers', '0')),
                        str(c.get('non_cmv_units', '0')),
                        c.get('physical_address'),
                        c.get('mailing_address'),
                        datetime.now().isoformat(),
                        c.get('mcs150_date'),
                        str(c.get('mcs150_mileage', '0')),
                        c.get('carrier_operation'), # array
                        c.get('cargo_carried'), # array
                        str(c.get('state_carrier_id', '')),
                        str(c.get('duns_number', '')),
                        c.get('safety_rating'),
                        c.get('safety_rating_date')
                    ))
                    
                    if len(records_batch) >= BATCH_SIZE:
                        await conn.executemany(query, records_batch)
                        total_inserted += len(records_batch)
                        count_in_file += len(records_batch)
                        records_batch = []
                
                if records_batch:
                    await conn.executemany(query, records_batch)
                    total_inserted += len(records_batch)
                    count_in_file += len(records_batch)
                
                print(f"  ✅ Finished {os.path.basename(file_path)}: {count_in_file} records.")

            except Exception as e:
                print(f"  ❌ Error processing file {file_path}: {e}")

        elapsed = time.time() - start_time
        print(f"\n✨ JSON Import Complete!")
        print(f"📊 Total Records Processed: {total_inserted}")
        print(f"⏱️ Time Elapsed: {elapsed:.2f} seconds")
        
    except Exception as e:
        print(f"❌ Critical Connection Error: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(import_from_json())
