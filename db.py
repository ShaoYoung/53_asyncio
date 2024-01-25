import asyncpg
import asyncio
from sshtunnel import SSHTunnelForwarder


class DB:
    def __init__(self, user, password, database, host):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.connection = None

    async def connect(self):
        # Соединение с сервером
        with SSHTunnelForwarder(
                ('92.62.151.39', 22),
                ssh_username="ShaoYoung",
                ssh_password="Ni4ki5ta",
                remote_bind_address=('localhost', 5432)) as server:
            server.start()
            print("server connected")
            # Соединение с БД
            self.connection = await asyncpg.connect(user=self.user, password=self.password, database=self.database, host=self.host)
            print(self.connection)
            print("database connected")

    async def execute(self, query, *args):
        print(self.connection)
        if self.connection is None:
            await self.connect()
        result = await self.connection.fetch(query, *args)
        return result

    async def close(self):
        if self.connection is not None:
            await self.connection.close()


if __name__ == '__main__':
    user = 'user01'
    password = 'dctvjue'
    host = 'localhost'
    database = 'gorbushka_test'
    db = DB(user=user, password=password, database=database, host=host)

    records_list = asyncio.run(db.execute("select category, count(*) from warehouse"))
    print(records_list)


