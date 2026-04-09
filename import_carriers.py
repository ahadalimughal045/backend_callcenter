import asyncio
import pandas as pd
import os
import time
from dotenv import load_dotenv
import asyncpg
from datetime import datetime

# Configuration
CSV_PATH = r"../call_dataset.csv"
CHUNK_SIZE = 5000  # Process 5k rows at a time
BATCH_SIZE = 1000  # Insert 1k rows per batch

async def import_carriers_csv():
    """Import carriers data from CSV to PostgreSQL carriers table"""
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        print("❌ DATABASE_URL not set in .env")
        return
    
    if not os.path.exists(CSV_PATH):
        print(f"❌ File not found at {CSV_PATH}")
        return

    print(f"🚀 Starting Carriers Import from {CSV_PATH}...")
    start_time = time.time()

    conn = await asyncpg.connect(db_url)
    try:
        # Create carriers table if it doesn't exist
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS carriers (
                id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                mc_number TEXT UNIQUE,
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
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        
        total_inserted = 0
        
        # Read CSV in chunks
        for chunk_idx, df_chunk in enumerate(pd.read_csv(
            CSV_PATH, 
            chunksize=CHUNK_SIZE, 
            low_memory=False,
            dtype=str
        )):
            print(f"\n📦 Processing chunk {chunk_idx + 1}...")
            
            # Process and prepare data
            records = []
            for idx, row in df_chunk.iterrows():
                try:
                    # Extract cargo columns (all columns starting with CRGO_)
                    cargo_cols = [col for col in df_chunk.columns if col.startswith('CRGO_') and row.get(col) == 'Y']
                    cargo_carried = cargo_cols if cargo_cols else None
                    
                    # Build physical address
                    phy_parts = [
                        row.get('PHY_STREET', ''),
                        row.get('PHY_CITY', ''),
                        row.get('PHY_STATE', ''),
                        row.get('PHY_ZIP', '')
                    ]
                    physical_address = ', '.join(filter(None, phy_parts)) or None
                    
                    # Build mailing address
                    mail_parts = [
                        row.get('CARRIER_MAILING_STREET', ''),
                        row.get('CARRIER_MAILING_CITY', ''),
                        row.get('CARRIER_MAILING_STATE', ''),
                        row.get('CARRIER_MAILING_ZIP', '')
                    ]
                    mailing_address = ', '.join(filter(None, mail_parts)) or None
                    
                    # Parse carrier operation
                    carrier_op_str = row.get('CARRIER_OPERATION')
                    carrier_op = carrier_op_str.split(',') if carrier_op_str and pd.notna(carrier_op_str) else None
                    
                    record = {
                        'mc_number': row.get('DOCKET1') or f"GEN_{idx}",  # Generate if missing
                        'dot_number': row.get('DOT_NUMBER') or '0',
                        'legal_name': row.get('LEGAL_NAME') or 'Unknown',
                        'dba_name': row.get('DBA_NAME') or None,
                        'entity_type': row.get('CLASSDEF') or None,
                        'status': row.get('STATUS_CODE') or None,
                        'email': row.get('EMAIL_ADDRESS') or None,
                        'phone': row.get('PHONE') or None,
                        'power_units': row.get('POWER_UNITS') or None,
                        'drivers': row.get('TOTAL_DRIVERS') or None,
                        'non_cmv_units': row.get('BUS_UNITS') or None,
                        'physical_address': physical_address,
                        'mailing_address': mailing_address,
                        'date_scraped': datetime.now().isoformat(),
                        'mcs150_date': row.get('MCS150_DATE') or None,
                        'mcs150_mileage': row.get('MCS150_MILEAGE') or None,
                        'operation_classification': None,  # Can be populated if needed
                        'carrier_operation': carrier_op,
                        'cargo_carried': cargo_carried,
                        'out_of_service_date': None,  # Not in CSV
                        'state_carrier_id': row.get('BUSINESS_ORG_ID') or None,
                        'duns_number': row.get('DUN_BRADSTREET_NO') or None,
                        'safety_rating': row.get('SAFETY_RATING') or None,
                        'safety_rating_date': row.get('SAFETY_RATING_DATE') or None,
                        'basic_scores': None,  # Can be populated separately
                        'oos_rates': None,  # Can be populated separately
                        'insurance_policies': None,  # Can be populated from Insurance.csv
                        'crashes': None,  # Can be populated separately
                        'inspections': None,  # Can be populated separately
                    }
                    
                    # Only add if we have required fields (mc_number or dot_number)
                    if record['mc_number'] or record['dot_number']:
                        records.append(record)
                
                except Exception as e:
                    print(f"⚠️  Error processing row {idx}: {e}")
                    continue
            
            # Insert in batches
            for batch_idx in range(0, len(records), BATCH_SIZE):
                batch = records[batch_idx:batch_idx + BATCH_SIZE]
                
                try:
                    # Insert with ON CONFLICT to handle duplicates
                    insert_query = """
                    INSERT INTO carriers (
                        mc_number, dot_number, legal_name, dba_name, entity_type, status,
                        email, phone, power_units, drivers, non_cmv_units,
                        physical_address, mailing_address, date_scraped, mcs150_date, mcs150_mileage,
                        operation_classification, carrier_operation, cargo_carried,
                        out_of_service_date, state_carrier_id, duns_number,
                        safety_rating, safety_rating_date, basic_scores, oos_rates,
                        insurance_policies, crashes, inspections
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                        $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
                        $23, $24, $25, $26, $27, $28, $29
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
                    WHERE carriers.mc_number IS NOT NULL
                    """
                    
                    for record in batch:
                        await conn.execute(
                            insert_query,
                            record['mc_number'],
                            record['dot_number'],
                            record['legal_name'],
                            record['dba_name'],
                            record['entity_type'],
                            record['status'],
                            record['email'],
                            record['phone'],
                            record['power_units'],
                            record['drivers'],
                            record['non_cmv_units'],
                            record['physical_address'],
                            record['mailing_address'],
                            record['date_scraped'],
                            record['mcs150_date'],
                            record['mcs150_mileage'],
                            record['operation_classification'],
                            record['carrier_operation'],
                            record['cargo_carried'],
                            record['out_of_service_date'],
                            record['state_carrier_id'],
                            record['duns_number'],
                            record['safety_rating'],
                            record['safety_rating_date'],
                            record['basic_scores'],
                            record['oos_rates'],
                            record['insurance_policies'],
                            record['crashes'],
                            record['inspections'],
                        )
                    
                    total_inserted += len(batch)
                    print(f"✅ Inserted batch: {total_inserted} total records")
                    
                except Exception as e:
                    print(f"❌ Error inserting batch: {e}")
                    continue
        
        elapsed = time.time() - start_time
        print(f"\n✨ Import Complete!")
        print(f"📊 Total Records Inserted: {total_inserted}")
        print(f"⏱️  Time Elapsed: {elapsed:.2f} seconds")
        print(f"📈 Records/sec: {total_inserted/elapsed:.2f}")
        
    except Exception as e:
        print(f"❌ Critical Error: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(import_carriers_csv())
