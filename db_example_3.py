import asyncpg
import asyncio
from sshtunnel import SSHTunnelForwarder


class DataBase:
    def __init__(self, user, password, database, host):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.connection = None

    async def fetch(self, query, *args):
        with SSHTunnelForwarder(
                ('92.62.151.39', 22),
                ssh_username="ShaoYoung",
                ssh_password="Ni4ki5ta",
                remote_bind_address=('localhost', 5432)) as server:
            server.start()
            print("server connected")
            self.connection = await asyncpg.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=server.local_bind_port)
            print("database connected")
            rows = await self.connection.fetch(query, *args)
            self.connection.close()
        return rows


# Пример использования
async def main():
    user = 'user01'
    password = 'dctvjue'
    host = 'localhost'
    database = 'gorbushka_test'
    db = DataBase(user=user, password=password, database=database, host=host)

    # Выполнение запроса
    query = "select category, count(*) from warehouse "
    where = "where warehouse_id=40 and balance>0 "
    group = "group by category order by category"
    query += where + group

    result = await db.fetch(query=query)

    # Обработка результата
    for row in result:
        print(row)

if __name__ == '__main__':
    asyncio.run(main())
