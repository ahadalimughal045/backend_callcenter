import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

async def test_trigger():
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Get a carrier with 0 OOS
    carrier = await conn.fetchrow("SELECT mc_number, oos_violation_total FROM carriers WHERE oos_violation_total = 0 LIMIT 1")
    if not carrier:
        print("No carrier with 0 OOS found")
        await conn.close()
        return
    
    mc = carrier['mc_number']
    print(f"Testing MC {mc}. Current OOS: {carrier['oos_violation_total']}")
    
    # Try to manually set it to 5
    print("Setting OOS to 5...")
    await conn.execute("UPDATE carriers SET oos_violation_total = 5 WHERE mc_number = $1", mc)
    
    # Check if it stayed 5
    updated = await conn.fetchrow("SELECT oos_violation_total FROM carriers WHERE mc_number = $1", mc)
    print(f"After update, OOS is: {updated['oos_violation_total']}")
    
    if updated['oos_violation_total'] == 0:
        print("TRIGGER REVERTED IT TO 0!")
    else:
        print("Trigger allowed manual override (unexpected based on my analysis).")
        
    await conn.close()

if __name__ == "__main__":
    asyncio.run(test_trigger())
