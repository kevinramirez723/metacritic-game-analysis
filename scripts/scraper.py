from bs4 import BeautifulSoup, SoupStrainer
import cchardet  # Improves parsing speed
import lxml  # Replaces default parser
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
    if game_pg.status_code == 404:  # Catches few cases where page is broken.
        data_dict["genres"].append([])
        data_dict["release_date"].append("")
        return
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
    if critic_pg.status_code == 404:  # Catches few cases where page is broken.
        data_dict["critics"].append({})
        return
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
                    critic = ""  # At least one instance of blank critic name.
            case 1:
                score = int(critic_data.get_text())
                score_dict[critic] = score
    data_dict["critics"].append(score_dict)

def store_metacritic_data() -> None:
    """Executes all scrape subroutines and exports unrefined results to csv.

    Current implementation stalls over time due to site limiting requests.
    Attempts at proxies via session.proxies or requests-ip-rotator still
    thwarted and further attempts of circumventing are beyond my current 
    skillset/time restriction. In the ideal non-rate limited scenario one
    should expect script to complete in <6 hours. If successful rapid
    request methods are found in the future I hope to multithread current
    scraper implementation.
    """
    DELAY = 5  # Assists with rate limiting in conjuction with Retry.
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
    retries = Retry(total=5, backoff_factor=10, status_forcelist=[429])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    pg_num = 0
    while True:
        try:
            game_urls = scrape_general_info(session, data_dict, pg_num)
            if not game_urls:
                break
            for i, url in enumerate(game_urls):
                scrape_genres_and_date(session, data_dict, url)
                sleep(DELAY)
                scrape_critic_scores(session, data_dict, url)
                sleep(DELAY)
                print(f"    Game {i} complete.")
            sleep(DELAY)
            print(f"Page {pg_num} processed...")
            pg_num += 1
        except Exception as e:
            # If data scraping is halted mid-progress one can continue
            # adding to previous csv by excising malformed chunk then
            # adjusting starting pg_num and setting to_csv to 'a' mode
            # with header=False. This must be done manually for the time
            # being, but could be automated.
            df = pd.DataFrame.from_dict(data_dict, orient="index").T
            rm_dups = lambda x: list(pd.unique(x)) if x is not None else None
            df["genres"] = df["genres"].apply(rm_dups)
            df.to_csv("../data/raw.csv", sep='|', index=False, quoting=3)
            print("\nEarly exit, something went wrong during operation:")
            print(getattr(e, "message", repr(e)))
            exit(1)
    df = pd.DataFrame(data_dict)
    df["genres"] = df["genres"].apply(lambda x: list(pd.unique(x)))
    df.to_csv("../data/raw.csv", sep='|', index=False, quoting=3)
    print("Successfully exported data.")

if __name__ == "__main__":
    store_metacritic_data()