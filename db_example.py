import asyncpg
import asyncio

class Database:
    def __init__(self, dsn):
        self.dsn = dsn
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn)

    async def disconnect(self):
        await self.pool.close()

    async def execute(self, query, *args):
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args)

    async def fetch(self, query, *args):
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args)

    async def fetchrow(self, query, *args):
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, *args)

    async def fetchval(self, query, *args):
        async with self.pool.acquire() as connection:
            return await connection.fetchval(query, *args)

    async def fetchrow(self, query, *args):
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, *args)

# Пример использования
async def main():
    db = Database('postgresql://username:password@localhost/database')
    await db.connect()

    # Выполнение запроса
    result = await db.fetch('SELECT * FROM users')

    # Обработка результата
    for row in result:
        print(row)

    await db.disconnect()

asyncio.run(main())