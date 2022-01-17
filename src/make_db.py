import sqlite3
import pandas as pd
import os

def create_imdb_title_principals(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS imdb_title_principals (
             idx INTEGER PRIMARY KEY,
             tconst TEXT,
             ordering INTEGER,
             nconst TEXT,
             category TEXT,
             job TEXT,
             characters TEXT
             );'''
    cursor.execute(sql)
    print('imdb_title_principals table created successfully....') 

def create_imdb_name_basic(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS imdb_name_basic (
             idx INTEGER PRIMARY KEY,
             nconst TEXT,
             birth_year REAL,
             death_year REAL,
             primary_profession TEXT,
             known_for_titles TEXT,
             FOREIGN KEY (nconst)
                 REFERENCES imdb_title_principals (nconst)
                 );'''
    cursor.execute(sql)
    print('imdb_name_basic table created successfully....')

def create_imdb_title_crew(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS imdb_title_crew (
             idx INTEGER PRIMARY KEY,
             tconst TEXT,
             directors TEXT,
             writers TEXT,
             FOREIGN KEY (tconst)
                 REFERENCES imdb_title_principals (tconst)
                 );'''
    cursor.execute(sql)
    print('imdb_title_crew table created successfully....') 

def create_imdb_title_ratings(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS imdb_title_ratings (
             idx INTEGER PRIMARY KEY,
             tconst TEXT,
             averagerating REAL,
             numvotes INTEGER,
             FOREIGN KEY (tconst)
                 REFERENCES imdb_title_principals (tconst)
                 );'''
    cursor.execute(sql)
    print('imdb_title_ratings table created successfully....') 

def create_imdb_title_basics(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS imdb_title_basics (
             idx INTEGER PRIMARY KEY,
             tconst TEXT,
             primary_title TEXT,
             original_title TEXT,
             start_year INTEGER,
             runtime_minutes REAL,
             genres TEXT,
             FOREIGN KEY (tconst)
                 REFERENCES imdb_title_principals (tconst)
                 );'''
    cursor.execute(sql)
    print('imdb_title_basics table created successfully....')  

def create_imdb_title_akas(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS imdb_title_akas (
             idx INTEGER PRIMARY KEY,
             title_id TEXT,
             ordering INTEGER,
             title TEXT,
             region TEXT,
             language TEXT,
             types TEXT,
             attributes TEXT,
             is_original_title REAL,
             FOREIGN KEY (title_id)
                 REFERENCES imdb_title_principals (tconst)
                 );'''
    cursor.execute(sql)
    print('imdb_title_akas table created successfully....') 

def create_tn_movie_budgets(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS tn_movie_budgets (
             idx INTEGER PRIMARY KEY,
             id INTEGER,
             release_date TEXT,
             movie TEXT,
             production_budget TEXT,
             domestic_gross TEXT,
             worldwide_gross TEXT,
             FOREIGN KEY (movie)
                 REFERENCES imdb_title_basics (primary_title)
             );'''
    cursor.execute(sql)
    print('tn_movie_budgets table created successfully....')  

def create_tmdb_movies(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS tmdb_movies (
             idx INTEGER PRIMARY KEY,
             genre_ids TEXT,
             id INTEGER,
             original_language TEXT,
             original_title TEXT,
             popularity REAL,
             release_date TEXT,
             title TEXT,
             vote_average REAL,
             vote_count INTEGER,
             FOREIGN KEY (title)
                 REFERENCES imdb_title_basics (primary_title)
             );'''
    cursor.execute(sql)
    print('tmdb_movies table created successfully....')  

def bom_movie_gross(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS bom_movie_gross (
             idx INTEGER PRIMARY KEY,
             title TEXT,
             studio TEXT,
             domestic_gross REAL,
             foreign_gross TEXT,
             year INTEGER,
             FOREIGN KEY (title)
                 REFERENCES imdb_title_basics (primary_title)
                 );'''
    cursor.execute(sql)
    print('bom_movie_gross table created successfully....')

def create_rotten_tomatoes_critic_reviews(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS rotten_tomatoes_critic_reviews (
             idx INTEGER PRIMARY KEY,
             rotten_tomatoes_link TEXT,
             critic_name TEXT,
             top_critic INTEGER,
             publisher_name TEXT,
             review_type TEXT,
             review_score TEXT,
             review_date TEXT,
             review_content TEXT
             );'''
    cursor.execute(sql)
    print('rotten_tomatoes_critic_reviews table created successfully....') 

def create_rotten_tomatoes_movies(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS rotten_tomatoes_movies (
             idx INTEGER PRIMARY KEY,
             rotten_tomatoes_link TEXT,
             movie_title TEXT,
             movie_info TEXT,
             critics_consensus TEXT,
             content_rating TEXT,
             genres TEXT,
             directors TEXT,
             authors TEXT,
             actors TEXT,
             original_release_date TEXT,
             streaming_release_date TEXT,
             runtime REAL,
             production_company TEXT,
             tomatometer_status TEXT,
             tomatometer_rating REAL,
             tomatometer_count REAL,
             audience_status TEXT,
             audience_rating REAL,
             audience_count REAL,
             tomatometer_top_critics_count INTEGER,
             tomatometer_fresh_critics_count INTEGER,
             tomatometer_rotten_critics_count INTEGER,
             FOREIGN KEY (rotten_tomatoes_link)
                 REFERENCES rotten_tomatoes_critic_reviews (rotten_tomatoes_link)
                 );'''
    cursor.execute(sql)
    print('rotten_tomatoes_movies table created successfully....')  

def create_tables(cursor):
    tables = list(cursor.execute("select name from sqlite_master where type is 'table'"))
    cursor.executescript(';'.join(["drop table if exists %s" %i for i in tables]))
    create_imdb_title_principals(cursor)
    create_imdb_name_basic(cursor)
    create_imdb_title_crew(cursor)
    create_imdb_title_ratings(cursor)
    create_imdb_title_basics(cursor)
    create_imdb_title_akas(cursor)
    create_tn_movie_budgets(cursor)
    create_tmdb_movies(cursor)
    bom_movie_gross(cursor)
    create_rotten_tomatoes_critic_reviews(cursor)
    create_rotten_tomatoes_movies(cursor)

name_map = {'imdb.title.crew.csv.gz': 'imdb_title_crew',
 'tmdb.movies.csv.gz': 'tmdb_movies',
 'imdb.title.akas.csv.gz': 'imdb_title_akas',
 'imdb.title.ratings.csv.gz': 'imdb_title_ratings',
 'imdb.name.basics.csv.gz': 'imdb_name_basics',
 'rotten_tomatoes_movies.csv.gz':'rotten_tomatoes_movies',
 'rotten_tomatoes_critic_reviews.csv.gz': 'rotten_tomatoes_critic_reviews',
 'imdb.title.basics.csv.gz': 'imdb_title_basics',
 'tn.movie_budgets.csv.gz': 'tn_movie_budgets',
 'bom.movie_gross.csv.gz': 'bom_movie_gross',
 'imdb.title.principals.csv.gz': 'imdb_title_principals'}



def create_movies_db():
    db_path = os.path.join('data', 'movies.db')
    conn = sqlite3.connect(db_path)  
    cursor = conn.cursor()
    create_tables(cursor) 
    print('=========================================================')
    for name in name_map:
        file_path = os.path.join('data', 'zippedData', name)
        print(f'Inserting data into the {name_map[name]} table....')
        pd.read_csv(file_path).to_sql(name_map[name], conn, if_exists='append', index_label='idx')
    print('=========================================================')
    print(f'Database created successfully!\nTo connect to the database: open a connection using the path "{db_path}"')