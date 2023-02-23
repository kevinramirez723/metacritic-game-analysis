from bs4 import BeautifulSoup, SoupStrainer
import cchardet
import lxml
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from time import sleep
from typing_extensions import TypedDict

class GameData(TypedDict):
    title: list[str]
    platform: list[str]
    release_date: list[str]
    metascore: list[str]
    userscore: list[str]
    genres: list[list[str]]
    critics: list[dict[str, int]]

def scrape_general_info(
    session: requests.sessions.Session,
    data_dict: GameData,
    pg_num: int=0,
) -> list[str]:
    """Scrapes game title, platform, metascore, and userscore.

    Also stores the each game's specific url for further data extraction.
    Note each page of the all games list contains a batch of 100 games.

    Args:
        session: A requests session object used in retrieving page content.
        data_dict: TypedDict used to store scraped data.
        pg_num: Page of the all games list to scrape.

    Returns:
        Urls for each game's standalone page. Needed to capture individual
        critic scores along with genre and release date info not summarized
        in the all games page.
    """
    ROOT = "https://www.metacritic.com"
    SELECT_CRITERIA = """
        a.title,
        .platform span.data,
        .clamp-metascore div,
        .clamp-userscore div
    """
    pg_url = ROOT + "/browse/games/score/metascore/all?page=" + str(pg_num)
    all_pg = session.get(pg_url)
    game_cells = SoupStrainer("td", class_="clamp-summary-wrap")
    games_html = BeautifulSoup(all_pg.content, "lxml", parse_only=game_cells)
    games_info = games_html.select(SELECT_CRITERIA)
    for i, game in enumerate(games_info):
        data = game.get_text(strip=True)
        match i % 4:
            case 0:
                data_dict["title"].append(data)
            case 1:
                data_dict["platform"].append(data)
            case 2:
                data_dict["metascore"].append(data)
            case 3:
                data_dict["userscore"].append(data)
    game_urls = [ROOT + game["href"] for game in games_info[::4]]
    return game_urls

def scrape_genres_and_date(
    session: requests.sessions.Session,
    data_dict: GameData,
    game_url: str
) -> None:
    """Scrape game's genres and release date.

    Args:
        session: A requests session object used in retrieving page content.
        data_dict: TypedDict used to store scraped data.
        game_url: Url for a specific game's main page.
    """
    game_pg = session.get(game_url)
    body = SoupStrainer("div", class_="left")
    game_html = BeautifulSoup(game_pg.content, "lxml", parse_only=body)
    genre_info = game_html.select("li.summary_detail.product_genre .data")
    release = game_html.select_one("li.summary_detail.release_data .data")
    genre_lst = [genre.get_text() for genre in genre_info]
    data_dict["genres"].append(genre_lst)
    data_dict["release_date"].append(release.get_text())

def scrape_critic_scores(
    session: requests.sessions.Session,
    data_dict: GameData,
    game_url: str
) -> None:
    """Scrapes scores and names from all critic reviews.

    Args:
        session: A requests session object used in retrieving page content.
        data_dict: TypedDict used to store scraped data.
        game_url: Url for a specific game's main page.
    """
    SUFFIX = "/critic-reviews"
    SELECT_CRITERIA = "div.review_critic, div.review_grade"
    critic_pg = session.get(game_url + SUFFIX)
    body = SoupStrainer("div", class_="body product_reviews")
    critic_html = BeautifulSoup(critic_pg.content, "lxml", parse_only=body)
    critic_cells = critic_html.select_one("ol.reviews.critic_reviews")
    critic_info = critic_cells.select(SELECT_CRITERIA)
    score_dict = {}
    for i, critic_data in enumerate(critic_info):
        match i % 2:
            case 0:
                name = critic_data.select_one("div.source")
                if name is not None:
                    critic = name.get_text()
                else:
                    critic = ""
            case 1:
                score = int(critic_data.get_text())
                score_dict[critic] = score
    data_dict["critics"].append(score_dict)

def store_metacritic_data() -> None:
    """Executes all scrape subroutines and exports unrefined results to csv.
    """
    data_dict = GameData(
        title=[],
        platform=[],
        release_date=[],
        metascore=[],
        userscore=[],
        genres=[],
        critics=[],
    )
    session = requests.Session()
    session.headers = {"User-Agent": "Edge"}
    retries = Retry(total=5, backoff_factor=10, status_forcelist=[404, 429])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    pg_num = 0
    while True:
        try:
            game_urls = scrape_general_info(session, data_dict, pg_num)
            if not game_urls:
                break
            for url in game_urls:
                scrape_genres_and_date(session, data_dict, url)
                sleep(.5)
                scrape_critic_scores(session, data_dict, url)
                sleep(.5)
            print(f"Page {pg_num} processed...")
            sleep(.5)
            pg_num += 1
        except Exception as e:
            df = pd.DataFrame.from_dict(data_dict, orient='index').T
            rm_dups = lambda x: list(pd.unique(x)) if x is not None else None
            df["genres"] = df["genres"].apply(rm_dups)
            df.to_csv("../data/raw.csv", sep='|', index=False, quoting=3)
            print("Early exit, something went wrong during operation.\n")
            print(getattr(e, 'message', repr(e)))
            exit(1)
    df = pd.DataFrame(data_dict)
    df["genres"] = df["genres"].apply(lambda x: list(pd.unique(x)))
    df.to_csv("../data/raw.csv", sep='|', index=False, quoting=3)
    print("Successfully exported data.")

if __name__ == "__main__":
    store_metacritic_data()