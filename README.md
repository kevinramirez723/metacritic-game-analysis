# SCRAPER CURRENTLY OUT OF DATE DUE TO UX REDESIGN ON METACRITIC
# SUBSEQUENT REVISION OF PROJECT PLANNED

# Metacritic Game Analysis
This is an exploratory data analysis done on various statistics derived from metacritic with an emphasis on genres and individual critic scoring.
Data set was gathered via [scraper.py](https://github.com/kevinramirez723/metacritic-game-analysis/blob/main/scripts/scraper.py) of my own design which stores a [raw.csv](https://github.com/kevinramirez723/metacritic-game-analysis/blob/main/data/raw.csv).
This csv is then formatted and cleaned in [sanitizer.py](https://github.com/kevinramirez723/metacritic-game-analysis/blob/main/scripts/sanitizer.py) to be exported as an optimized [parquet](https://github.com/kevinramirez723/metacritic-game-analysis/blob/main/data/refined.parquet) file.
Data set (currently incomplete) last updated on: 2/23/2023.
The final data set is of the form:

<table class="tg">
<thead>
  <tr>
    <th class="tg-0lax"></th>
    <th class="tg-c3ow" colspan="5">general</th>
    <th class="tg-baqh" colspan="3">genres</th>
    <th class="tg-baqh" colspan="3">critics</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-0lax"></td>
    <td class="tg-c3ow">title</td>
    <td class="tg-c3ow">platform</td>
    <td class="tg-c3ow">release_date</td>
    <td class="tg-c3ow">metascore</td>
    <td class="tg-c3ow">userscore</td>
    <td class="tg-0lax">Action</td>
    <td class="tg-baqh">...</td>
    <td class="tg-0lax">Fantasy</td>
    <td class="tg-0lax">Gameshark</td>
    <td class="tg-baqh">...</td>
    <td class="tg-0lax">Eurogamer</td>
  </tr>
  <tr>
    <td class="tg-0lax">0</td>
    <td class="tg-baqh">str</td>
    <td class="tg-baqh">cat</td>
    <td class="tg-baqh">datetime</td>
    <td class="tg-baqh">int8</td>
    <td class="tg-baqh">float</td>
    <td class="tg-baqh">bool</td>
    <td class="tg-0lax"></td>
    <td class="tg-baqh">bool</td>
    <td class="tg-baqh">int8</td>
    <td class="tg-0lax"></td>
    <td class="tg-baqh">int8</td>
  </tr>
</tbody>
</table>

All required modules for scraping, cleaning, and analyzing can be installed with `pip install -r requirements.txt`.

Subsequent report based on data findings is stored in analysis_results folder along with any figures generated from analysis.ipynb and other misc. resources used.
