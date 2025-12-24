import asyncio
from pathlib import Path

from eclipse24.libs.db.postgres import init_pool, close_pool, execute

SQL_PATH = Path(__file__).resolve().parents[2] / "configs" / "sql" / "001_init.sql"


async def main():
    await init_pool()
    sql = SQL_PATH.read_text()
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        await execute(stmt + ";")
    await close_pool()
    print("✅ migration finished")


if __name__ == "__main__":
    asyncio.run(main())
