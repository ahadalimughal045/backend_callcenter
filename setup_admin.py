import asyncio
import os
import bcrypt
import asyncpg
from dotenv import load_dotenv

async def setup_admin():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not found")
        return
    
    email = "admin@freightintel.ai"
    password = "AdminPassword123"
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    
    conn = await asyncpg.connect(db_url)
    try:
        # Create user if not exists, or update password if it does
        await conn.execute("""
            INSERT INTO users (user_id, name, email, password_hash, role, plan, daily_limit, is_online)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (email) DO UPDATE SET password_hash = EXCLUDED.password_hash, role = 'admin'
        """, "admin-1", "Admin User", email.lower(), password_hash, "admin", "Enterprise", 100000, False)
        
        print("\n✅ Admin user setup successfully!")
        print(f"📧 Email: {email}")
        print(f"🔑 Password: {password}")
        print("⚠️  Ensure PostgreSQL is running before use.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(setup_admin())
