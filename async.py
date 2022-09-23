import asyncio
import aiohttp
from datetime import datetime
from more_itertools import chunked
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

CHUCK_SIZE = 10

DB_SERVER = 'localhost'
DB_USER = 'srolnik'
DB_PASSWORD = ''
DB_NAME = 'suppliers'
DB_PORT = 5432
PG_DSN = f'postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}:{DB_PORT}/{DB_NAME}'

engine = create_async_engine(PG_DSN)
Base = declarative_base()


class People(Base):
    __tablename__ = 'people'
    id = Column(Integer, primary_key=True)  # ID персонажа
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)  # строка с названиями фильмов через запятую
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)  # строка с названиями типов через запятую
    starships = Column(String)  # строка с названиями кораблей через запятую
    vehicles = Column(String)  # строка с названиями транспорта через запятую


async def get_people(session, people_id):
    result = await session.get(f'https://swapi.dev/api/people/{people_id}')
    return await result.json()


async def main():
        async with engine.begin() as conn:  # создание соединения
            await conn.run_sync(Base.metadata.create_all)  # создание таблиц
            await conn.commit()
        async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with aiohttp.ClientSession() as web_session:
            for chunk_id in chunked(range(1, 20), CHUCK_SIZE):
                coros = [get_people(web_session, i) for i in chunk_id]
                result = await asyncio.gather(*coros)

                # --- запись данных в БД ---
                people_list = [People(
                        birth_year=item.get('birth_year'),
                        eye_color=item.get('eye_color'),
                        films=','.join(item.get('films', [])),
                        gender=item.get('gender'),
                        hair_color=item.get('hair_color'),
                        height=item.get('height'),
                        homeworld=item.get('homeworld'),
                        mass=item.get('mass'),
                        name=item.get('name'),
                        skin_color=item.get('skin_color'),
                        species=','.join(item.get('species', [])),
                        starships=','.join(item.get('starships', [])),
                        vehicles=','.join(item.get('vehicles', []))
                    )
                    for item in result]

                async with async_session_maker() as orm_session:
                    orm_session.add_all(people_list)
                    await orm_session.commit()

start = datetime.now()
asyncio.run(main())
end = datetime.now()
print(end - start)
