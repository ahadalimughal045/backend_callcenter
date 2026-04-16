import asyncio
import json
from app.database import connect_db, get_pool, close_db

async def check():
    await connect_db()
    pool = get_pool()
    
    # Check carriers with inspections or crashes (Safety data)
    counts = await pool.fetchrow('''
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER(WHERE safety_rating IS NOT NULL AND safety_rating != '') as with_rating,
            COUNT(*) FILTER(WHERE crash_count > 0) as with_crashes,
            COUNT(*) FILTER(WHERE inspection_count > 0) as with_inspections,
            COUNT(*) FILTER(WHERE oos_violation_total > 0) as with_oos
        FROM carriers
    ''')
    
    print(json.dumps(dict(counts), indent=2))
    await close_db()

if __name__ == "__main__":
    asyncio.run(check())
