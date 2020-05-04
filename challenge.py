# -- Module 8 Challenge
# -- Objective: 
#    - Create an automated ETL pipeline.
#    - Extract data from multiple sources.
#    - Clean and transform the data automatically using Pandas and regular expressions.
#    - Load new data into PostgreSQL.

# -- Importing and loading dependencies.
import json
import pandas as pd
import numpy as np 
import re 
from sqlalchemy import create_engine 
import time 
import os 

# -- Import Postgres SQL password from Config file. 
from config import db_password

# -- NOTE: Please ensure that the following files are within 
#    the same folder level as Challenge.ipynb for import:
#    - movies_metadata.csv
#    - ratings.csv 
#    - wikipedia.movies.json

# -------------------------------------------------------- 

# -- Extract Process --

# -- File Directory
# -- Using os.path.join to avoid "/" or "\" errors.
file_dir = os.path.join("")

# -- Loading Files, adding Try-Except blocks to catch and warn
#    user if files are missing/not in correct location. 
try:
    with open(f'{file_dir}wikipedia.movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
except FileNotFoundError:
    print(f"Wikipedia.Movies.json file not found. \n\nPlease ensure file is placed within same folder level as Challenge.ipynb file.")

try: 
    kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
except FileNotFoundError:
    print(f"Kaggle_Metadata.csv file not found. \n\nPlease ensure file is placed within same folder level as Challenge.ipynb file.")

try:
    ratings = pd.read_csv(f'{file_dir}ratings.csv', low_memory=False)
except FileNotFoundError:
    print(f"Ratings.csv file not found. \n\nPlease ensure file is placed within same folder level as Challenge.ipynb file.")

# --------------------------------------------------------     

# -- Requirements: 
#    - Create a function that successfully takes in three arguments: 
#      Wikipedia data, Kaggle metadata, and MovieLens rating data. 
# 
#    - Function performs all transformation steps, with no errors.
#
#    - Function performs all load steps with no errors. Additionally, 
#      existing data is removed from SQL and tables are not removed from SQL.

def ETL_MovieData(wiki_movies_raw, kaggle_metadata, ratings): 

    # -- Transform Process --

    # -- Added copy function here to eliminate future table error.
    wiki_movies_df = pd.DataFrame(wiki_movies_raw).copy()

    #-- Filtering expression for only movies with a director and IMDB link.
    wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]

    # -- Create new dataframe from filtered list.
    wiki_movies_df = pd.DataFrame(wiki_movies)

    # -- Function to handle alternative movie titles and consolidate
    #    columns with the same data to one column. 
    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
        # combine alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie

    # -- Running function to clean alternative movie titles and 
    #    consolidate columns with the same data to one column. 
    clean_movies = [clean_movie(movie) for movie in wiki_movies] 

    wiki_movies_df = pd.DataFrame(clean_movies)

    # -- Extracting IMDB ID from link to merge with Kaggle Data.
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')

    # -- Removing duplicate IMDB IDs. 
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep].copy()
    
# --------------------------------------------------------    

    # -- Create Box Office data frame from Wiki Movies data frame values 
    #    and drop missing Box Office Values.
    box_office = wiki_movies_df['Box office'].dropna() 

    # -- Convert all Box Office data to strings.
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

    # -- Extracting and converting Box Office Values

    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan
    
    # -- Parse Box Office data
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illion'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'

    # -- Remove any values between a dollar sign and a hyphen (for budgets given in ranges).
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    # -- Extracting cleaned Box Office values and adding them to Wiki Movies data frame.
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    # -- Dropping (old) Box Office column.
    wiki_movies_df.drop('Box office', axis=1, inplace=True)    
    
# --------------------------------------------------------     

    # -- Create Budget data frame from Wiki Movies data frame values 
    #    and drop missing Budget Values.
    budget = wiki_movies_df['Budget'].dropna()

    # -- Convert all Budget data to strings.
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)

    # -- Remove any values between a dollar sign and a hyphen (for budgets given in ranges).
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    # -- Remove citation references 
    budget = budget.str.replace(r'\[\d+\]\s*', '')

    # -- Parsing Budget Data.
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars) 

    # -- Dropping (old) Budget column.
    wiki_movies_df.drop('Budget', axis=1, inplace=True) 

# --------------------------------------------------------     

    # -- Parsing Release Date.

    # -- Variable to hold all non-null values of Release date in the DataFrame, converting lists to strings. 
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    # -- Release Date Forms.
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'

    # -- Extract and parse dates to Wiki Movies data frame.
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

# --------------------------------------------------------  

    # -- Convert all non-null Release Date data to strings in new data frame.

    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    # -- Extracting digits with capture groups. 
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

    # -- Converting strings to numeric values. 
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

    # -- Convert hour/minute capture groups.
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

    # -- Dropping (old) Box Office column.
    wiki_movies_df.drop('Running time', axis=1, inplace=True)

# -------------------------------------------------------- 
    
    ## -- Cleaning Kaggle Data 

    # -- Keeping rows where "Adult" column is false, and then dropping "Adult" column.
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns') 

    # -- Create boolean values for video column. 
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'

    # -- Convert column data.
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')

    # -- Convert release date to datetime format. 
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
# -------------------------------------------------------- 

    ## -- Merging Wiki and Kaggle data
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle']) 


    # -- Drop merged rows from data frame 
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)

    # -- Drop competing columns 
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True) 

    # -- Function to fill in missing data for column pair and drops
    #    redundant column 
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)
    
    # -- Consolidating Kaggle Data. 
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    # -- Dropping Video column for it only displays one value. 
    movies_df.drop('video', axis=1, inplace=True)

    # -- Reordering columns in dataframe. 
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]

    # -- Renaming columns
    movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                     }, axis='columns', inplace=True)
    
# --------------------------------------------------------     
    
    ## -- Checking Ratings Data

    # -- Converting "timestamp" to datetime and assigning it back to column. 
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

    # -- Pivoting and shifting Ratings data 
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')

    # -- Renaming Ratings columns 
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

    # -- Merging Movie data with Ratings data 
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

    # -- Filling missing ratings values 
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

# --------------------------------------------------------     

    # -- Load Process --

    # -- Import connection string to Local Server

    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"

    # -- Creating database engine 
    engine = create_engine(db_string) 

    # -- Import Movie data to Database
    #    Added If statement to check if table exists in database. If exists, 
    #    truncate all rows to remove existing data before importing in new data.
    #    If table does not exist in database, then it will create table and 
    #    begin importing rows. 
    #    Resource: https://stackoverflow.com/questions/33053241/sqlalchemy-if-table-does-not-exist 

    # -- Importing Movie Data

    if engine.dialect.has_table(engine, 'movies'):
        print(f"'Movies' table exists. Truncate table before import.")
        
        engine.execute("TRUNCATE TABLE movies")
        print(f"Table truncate completed.\n")
        
        movies_df.to_sql(name='movies', con=engine, if_exists='append')
        print(f"Movie data successfully imported.\n")
    else:
        movies_df.to_sql(name='movies', con=engine)
        print(f"Movie data successfully imported.\n")

    # -- Importing Ratings Data
    rows_imported = 0

    # get the start_time from time.time()
    start_time = time.time()

    for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        rows_imported += len(data)

        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')

# -- END OF FUNCTION --

# -- Run complete ETL process. 
ETL_MovieData(wiki_movies_raw, kaggle_metadata, ratings)