#!/usr/bin/env python

from fastapi import FastAPI
import sqlite3
import asyncio.queues

app = FastAPI()

# Connection pool
dbpool = asyncio.queues.Queue()
for i in range(10):
    dbpool._queue.append(sqlite3.connect('movies.db'))


@app.get('/details/company')
async def get_company_details(production_id: int, year: int):
    db: sqlite3.Connection = await dbpool.get()
    try:
        cur = db.execute(

            """SELECT
            mc.company_id,
            c.name,
            year,
            SUM(m.budget) AS budget,
            SUM(m.revenue) AS revenue
        FROM
            movies AS m
            LEFT JOIN
                movie_companies AS mc ON mc.movie_id = m.id
            INNER JOIN
                companies AS c ON mc.company_id = c.id
        WHERE company_id = ? AND year = ?
        GROUP BY mc.company_id, year""", (production_id, year)

        )

        try:
            r = cur.fetchone()
            if not r:
                return {}
            return {'budget': r[3], 'revenue': r[4]}
        finally:
            cur.close()
    finally:
        await dbpool.put(db)


@app.get('/details/genre')
async def get_genre_details(year: int):
    db: sqlite3.Connection = await dbpool.get()
    try:
        cur = db.execute(

            """SELECT
            g.id, g.name, year, COUNT(mg.id) AS count
            FROM
	        movies AS m
	        LEFT JOIN movie_genres AS mg ON m.id = mg.movie_id
	        INNER JOIN genres AS g ON g.id = mg.genre_id
            WHERE year = ?
            GROUP BY g.id
            ORDER BY count DESC
            LIMIT 1""", (year,)

        )

        try:
            r = cur.fetchone()
            if not r:
                return {}
            return {'genre': r[1]}
        finally:
            cur.close()
    finally:
        await dbpool.put(db)
