import asyncpg
import asyncio
from sshtunnel import SSHTunnelForwarder

import config


# Устанавливаем соединение с базой данных. Так не будет работать, т.к. в SSHTunnelForwarder используется менеджер контекста
# async def connect_to_db():
#     # Соединение с сервером
#     with SSHTunnelForwarder(
#             ('92.62.151.39', 22),
#             ssh_username="ShaoYoung",
#             ssh_password="Ni4ki5ta",
#             remote_bind_address=('localhost', 5432)) as server:
#         server.start()
#         print("server connected")
#         # Соединение с БД
#         # return await asyncpg.connect(database=config.DATABASE, user=config.PGUSER,
#         #                              password=config.PGPASSWORD, host=config.PGHOST, port=config.PGPORT)
#         return await asyncpg.connect(database='gorbushka_test', user='user01', password='dctvjue', host='localhost', port=server.local_bind_port)
#
#         # conn = await asyncpg.connect(database='gorbushka_test', user='user01', password='dctvjue', host='localhost', port=server.local_bind_port)


async def execute(query):
    # Соединение с сервером
    # Нельзя разрывать SSH соединение до окончания сессии с БД
    with SSHTunnelForwarder(
            ('92.62.151.39', 22),
            ssh_username="ShaoYoung",
            ssh_password="Ni4ki5ta",
            remote_bind_address=('localhost', 5432)) as server:
        server.start()
        print("server connected")

        # Соединение с БД
        # return await asyncpg.connect(database=config.DATABASE, user=config.PGUSER,
        #                              password=config.PGPASSWORD, host=config.PGHOST, port=config.PGPORT)

        conn = await asyncpg.connect(database='gorbushka_test', user='user01', password='dctvjue', host='localhost', port=server.local_bind_port)

        # result = await conn.fetchval(query)
        # result = await conn.fetchval('SELECT name FROM product_type WHERE type = "category"')
        # res_string = await conn.fetchval(query)
        records_list = await conn.fetch(query)
        await conn.close()
    return records_list


# поиск всех категорий устройств
async def select_category():
    conn = await connect_to_db()
    result = await conn.fetchval('SELECT name FROM product_type WHERE type = "category"')
    await conn.close()
    return result is not None


# поиск id категории по ее имени
async def select_category_id(name):
    conn = await connect_to_db()
    result = await conn.fetchval('SELECT id FROM product_type WHERE name = $1', name)
    await conn.close()
    return result is not None


# поск вендоров по категории товара
async def select_vendor(category_id):
    conn = await connect_to_db()
    result = await conn.fetchval('SELECT name FROM product WHERE category_id = $1', category_id)
    result = list(set(result))
    await conn.close()
    return result is not None


# # Создаем таблицу пользователей, если её еще нет
# async def create_user_table():
#     conn = await connect_to_db()
#     await conn.execute('''
#         CREATE TABLE IF NOT EXISTS our_users(
#             user_id BIGINT PRIMARY KEY,
#             user_name TEXT,
#             user_full_name TEXT
#         )
#     ''')
#     await conn.close()

#
# # Функция, чтобы проверить, существует ли пользователь
# async def check_user_exists(user_id):
#     conn = await connect_to_db()
#     result = await conn.fetchval('SELECT user_id FROM our_users WHERE user_id = $1', user_id)
#     await conn.close()
#     return result is not None
#
#
# # Функция, чтобы добавить нового пользователя
# async def add_new_user(user_id, user_name, user_full_name):
#     conn = await connect_to_db()
#     await conn.execute('INSERT INTO our_users(user_id, user_name,  user_full_name) VALUES($1, $2, $3)',
#                        user_id, user_name, user_full_name)
#     await conn.close()
#
if __name__ == '__main__':
    print('TEST')
    query = "select category, count(*) from warehouse "
    where = "where warehouse_id=40 and balance>0 "
    group = "group by category order by category"
    query += where + group
    # result = execute(query)
    # print(result)
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(connect_to_db(query))
    records_list = asyncio.run(execute(query))
    print(records_list)
