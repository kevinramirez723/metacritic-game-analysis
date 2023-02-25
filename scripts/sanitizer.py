from ast import literal_eval
from os.path import isfile
import pandas as pd
from scraper import store_metacritic_data

def expand_critics_col(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Takes column of dictionary like strings and converts to dataframe
    of sparse columns based on the keys (critic names).

    Args:
        df: Main dataframe containing critics column to expand.

    Returns:
        A tuple containing original dataframe updated to exclude expanded
        column and the newly constructed expanded critics dataframe.
    """
    df.critics = df.critics.apply(literal_eval)
    expanded_critics = pd.json_normalize(df.critics).set_index(df.index)
    df = df.drop("critics", axis=1)
    return df, expanded_critics

def multiencode_genres(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Takes a column of list like strings and converts to dataframe where
    each unique element found throughout the entire original column is
    given its own sparse column.

    Very infrequent unique values (i.e. uncommon genre labels) are
    removed to limit amount of new columns generated.

    Args:
        df: Main dataframe containing genres column to encode.

    Returns:
        A tuple containing original dataframe updated to exclude encoded
        column and the newly constructed encoded genres dataframe.
    """
    THRESHOLD = 85
    df.genres = df.genres.apply(literal_eval)
    exploded = df.genres.explode().where(lambda x: x != "").dropna()
    count = exploded.value_counts()
    # Keep most common genre labels
    prevalent = exploded[exploded.isin(count[count > THRESHOLD].index)]
    multiencoded_genres = pd.crosstab(prevalent.index, prevalent)
    df = df.drop("genres", axis=1)
    return df, multiencoded_genres

def sanitize_and_save():
    """Processes raw csv of scraped data into format more agreeable for
    intended analysis. This includes improving storage method by
    optimizing data types and exporting as parquet.

    Will launch scraper if local copy of raw is unable to be found.
    """
    if not isfile("../data/raw.csv"):
        print("Local raw copy missing.\nPreparing to scrape...")
        store_metacritic_data()
        print("Raw restoration complete.")
    mcdf = pd.read_csv(
        "../data/raw.csv",
        sep='|',
        dtype={
            "title": "string",
            "platform": "category",
            "metascore": "int8",
        }
    )
    mcdf.userscore = pd.to_numeric(mcdf.userscore, errors="coerce")
    mcdf = mcdf.dropna()  # Removes rows missing any data
    mcdf, genres_df = multiencode_genres(mcdf)
    mcdf, critics_df = expand_critics_col(mcdf)
    # Group subcolumns
    multi_indexed = {
        "general": mcdf,
        "genres": genres_df,
        "critics": critics_df
    }
    mcdf = pd.concat(multi_indexed.values(), axis=1, keys=multi_indexed.keys())
    # Optimize remaining unspecified dtypes
    mcdf.critics = mcdf.critics.fillna(-1).astype("int8")
    mcdf.genres = mcdf.genres.astype("bool")
    tformatted = pd.to_datetime(mcdf.general.release_date, format="%b %d, %Y")
    mcdf["general", "release_date"] = tformatted
    mcdf.to_parquet("../data/refined.parquet", compression="gzip")

if __name__ == "__main__":
    sanitize_and_save()
