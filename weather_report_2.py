import asyncio
import time
from aiohttp import ClientSession


async def get_weather(city):
    async with ClientSession() as session:
        url = f'http://api.openweathermap.org/data/2.5/weather'
        params = {'q': city, 'APPID': '2a4ff86f9aaa70041ec8e82db64abf56'}

        async with session.get(url=url, params=params) as response:
            weather_json = await response.json()
            return f'{city}: {weather_json["weather"][0]["main"]}'


async def main(cities_):
    tasks = []
    for city in cities_:
        tasks.append(asyncio.create_task(get_weather(city)))

    # А может ли асинхронная функция не просто что-то делать внутри себя (например, запрашивать и выводить в консоль погоду),
    # но и возвращать результат? Ту же погоду, например, чтобы дальнейшей обработкой занималась функция верхнего уровня main()
    # Нет ничего проще.
    # Только в этом случае для группового запуска задач необходимо использовать уже не цикл с await, а функцию asyncio.gather
    results = await asyncio.gather(*tasks)

    for result in results:
        print(result)


cities = ['Tomsk', 'Moscow', 'St. Petersburg', 'Rostov-on-Don', 'Kaliningrad', 'Vladivostok',
          'Minsk', 'Beijing', 'Delhi', 'Istanbul', 'Tokyo', 'London', 'New York']

print(time.strftime('%X'))

asyncio.run(main(cities))

print(time.strftime('%X'))