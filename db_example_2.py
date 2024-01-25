import asyncpg
import asyncio
from sshtunnel import SSHTunnelForwarder


class DataBase:
    # def __init__(self, dsn):
    #     self.dsn = dsn
    #     self.pool = None
    def __init__(self, user, password, database, host):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.pool = None

    async def connect(self):
        # self.pool = await asyncpg.create_pool(dsn=self.dsn)
        # Соединение с БД
        self.pool = await asyncpg.connect(user=self.user, password=self.password, database=self.database, host=self.host)

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


# Пример использования
async def main():
    user = 'user01'
    password = 'dctvjue'
    host = 'localhost'
    database = 'gorbushka_test'
    db = DataBase(user=user, password=password, database=database, host=host)

    # Соединение с сервером
    with SSHTunnelForwarder(
            ('92.62.151.39', 22),
            ssh_username="ShaoYoung",
            ssh_password="Ni4ki5ta",
            remote_bind_address=('localhost', 5432)) as server:

        server.start()
        print("server connected")

        # db = DataBase('postgresql://username:password@localhost/database')
        await db.connect()
        print("database connected")

        # Выполнение запроса
        query = "select category, count(*) from warehouse "
        where = "where warehouse_id=40 and balance>0 "
        group = "group by category order by category"
        query += where + group

        # result = await db.fetch('SELECT * FROM users')
        result = await db.fetch(query=query)

        # Обработка результата
        for row in result:
            print(row)

        await db.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
