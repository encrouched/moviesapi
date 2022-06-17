#!/usr/bin/env python

import csv
import sqlite3
import ast
from typing import Optional


def list_validator(s: Optional[str]) -> list:
    if s is None:
        return []
    try:
        return ast.literal_eval(s)
    except SyntaxError:
        print(f'Skipping invalid payload {s}...')
        return []

# sys.stdout.reconfigure(encoding='utf-8')

# import os
# os.system('rm -f movies.db')

with sqlite3.connect('movies.db') as db:
    with open('schema.sql', 'r') as f:
        schema = f.read()

    cur = db.cursor()
    try:
        _ = db.executescript(schema)
    finally:
        cur.close()

    with open('data/the-movies-dataset/movies_metadata.csv', 'r', newline='', encoding='utf-8') as f:
        r = csv.DictReader(f, dialect=csv.unix_dialect)

        for movie in r:

            try:
                int(movie['id'])
            except ValueError:
                print(f"Skipping {movie['id']}, invalid id")
                continue

            title = movie.get('title')
            if not title:
                title = movie.get('original_title')

            if not title:
                print(f"Skipping {movie['id']}, missing title...")
                continue

            year = movie['release_date']
            if year:
                year = year[:4]

            cur = db.cursor()
            try:
                # Ignore id conflicts, they're actual duplicates
                _ = cur.execute("""
INSERT INTO movies(id, title, year, budget, revenue, popularity)
VALUES(?, ?, ?, COALESCE(?, 0), COALESCE(?, 0), COALESCE(?, 0))
ON CONFLICT DO NOTHING 
""", (int(movie['id']), title, year, movie['budget'], movie['revenue'], movie['popularity']))
            finally:
                cur.close()

            companies = list_validator(movie.get('production_companies'))

            for c in companies:
                cur = db.cursor()
                try:
                    # Ignore id conflicts, they're actual duplicates
                    _ = cur.execute("""
INSERT INTO companies(id, name)
VALUES(?, ?)
ON CONFLICT DO NOTHING 
""", (c['id'], c['name']))
                finally:
                    cur.close()

                cur = db.cursor()
                try:
                    _ = cur.execute("""
INSERT INTO movie_companies(movie_id, company_id)
VALUES(?, ?)
ON CONFLICT DO NOTHING 
""", (int(movie['id']), c['id']))
                finally:
                    cur.close()

            genres = list_validator(movie.get('genres'))

            for g in genres:
                cur = db.cursor()
                try:
                    _ = cur.execute("""
INSERT INTO genres(id, name)
VALUES(?, ?)
ON CONFLICT DO NOTHING 
""", (int(g['id']), g['name']))
                finally:
                    cur.close()

                cur = db.cursor()
                try:
                    _ = cur.execute("""
INSERT INTO movie_genres(movie_id, genre_id)
VALUES(?, ?)
ON CONFLICT DO NOTHING 
""", (int(movie['id']), g['id']))
                finally:
                    cur.close()

    db.commit()
