import asyncio
import asyncpg
import os
from dotenv import load_dotenv

async def check():
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
    db_url = os.getenv("DATABASE_URL")
    conn = await asyncpg.connect(db_url)
    count = await conn.fetchval("SELECT count(*) FROM carriers")
    print(f"TOTAL CARRIERS IN DB: {count:,}")
    # Get a sample
    sample = await conn.fetch("SELECT legal_name, dot_number FROM carriers LIMIT 5")
    for r in sample:
        print(f" - {r['legal_name']} (DOT: {r['dot_number']})")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check())
