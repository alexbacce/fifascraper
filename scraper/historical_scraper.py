import datetime
import re
from multiprocessing.pool import Pool

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

LAST_UPDATE_PER_FIFA_VERSION = {16: 58, 17: 99, 18: 84, 19: 75, 20: 8}


def retrieve_raw_player_attributes(base_url, offset):
    """Retrieve the raw html of the attributes of the player

    Parameters
    ----------
    base_url: str
        The url to which make the request
    offset: int
        The offset to use when making the request

    Returns
    -------
    plain_text: str
        The raw html containing the attributes of the players
    """
    url = base_url + str(offset)
    source_code = requests.get(url)
    plain_text = source_code.text
    return plain_text


def extract_player_attributes(plain_text, player_attributes):
    """From the raw html, extract the player attributes

    Parameters
    ----------
    plain_text: str
        The raw html containing the player attributes
    player_attributes: list
        The list of player attributes to retrieve

    Returns
    -------
    player_attributes_df: pd.DataFrame
        The DataFrame containing the attributes of the players extracted from the html
    """
    soup = BeautifulSoup(plain_text, 'html.parser')
    table_body = soup.find('tbody')
    players_attributes_df = pd.DataFrame(columns=player_attributes)

    for row in table_body.findAll('tr'):
        td = row.findAll('td')
        pid = td[0].find('img').get('id')
        nationality = td[1].find('a').get('title')
        name = td[1].findAll('a')[1].text
        overall = td[3].text.strip()
        potential = td[4].text.strip()
        club = td[5].find('a').text
        value = td[6].text.strip()
        player_data = pd.DataFrame([[pid, name, nationality, overall, potential, club, value]],
                                   columns=player_attributes)

        players_attributes_df = players_attributes_df.append(player_data, ignore_index=True)

    return players_attributes_df


def _source_update_version_is_valid(plain_text, fifa_version):
    """Check if the retrieved statistics match the FIFA version and the FIFA update

    Parameters
    ----------
    plain_text: str
        The raw html
    fifa_version: int
        The FIFA version to verify

    Returns
    -------
    is_true: bool
        True if the statistics are from the given fifa_version and update_version, false otherwise
    """
    soup = BeautifulSoup(plain_text, 'html.parser')
    retrieved_fifa_version = soup.find('div', {'class': 'carousel-cell is-initial-select selected'})
    parsed_fifa_version = re.findall('^FIFA (\d+)', retrieved_fifa_version.text)

    if len(parsed_fifa_version) != 1:
        raise RuntimeError(f"The FIFA version was not extracted from {retrieved_fifa_version.text}")

    return str(fifa_version) == parsed_fifa_version[0]


def _parse_date(str_date):
    """Parse the string containing the date to a datetime object

    Parameters
    ----------
    str_date: str
        The string containing the date

    Returns
    -------
    datetime_date: datetime.datetime
        The converted date
    """
    date_time_obj = datetime.datetime.strptime(str_date, '%d %b  %Y')
    return date_time_obj


def _parse_fifa_update_date(plain_text):
    """Extract from the ra

    Parameters
    ----------
    plain_text

    Returns
    -------

    """
    soup = BeautifulSoup(plain_text, 'html.parser')
    retrieved_fifa_version = soup.find('div', {'class': 'carousel-cell is-initial-select selected'})

    month_year_fifa_update = re.findall('^FIFA \d{2} (.* \d{4}).*$', retrieved_fifa_version.text)[0]
    day_fifa_update = retrieved_fifa_version.find('a', {'class': 'bp3-tag p bp3-intent-primary'}).text

    complete_date = day_fifa_update + " " + month_year_fifa_update

    return _parse_date(complete_date)


def _compute_id_splits(list_to_split, n_split):
    """Divide a list into n_split parts

    Parameters
    ----------
    list_to_split: List
        The list to split
    n_split: int
        The number of splits to do

    Returns
    -------
    splitted_list: List
        A list containing the splitted list
    """
    return np.array_split(np.array(list_to_split), n_split)


def _extract_stats(plain_text, player_statistics, date):
    soup = BeautifulSoup(plain_text, 'html.parser')
    skill_map = {}
    columns = soup.find('div', {'class': 'teams'}).find('div', {'class': 'columns'}).findAll('div',
                                                                                             {'class': 'column col-4'})
    for column in columns:
        skills = column.findAll('li')
        for skill in skills:
            if (skill.find('label') != None):
                label = skill.find('label').text
                value = skill.text.replace(label, '').strip()
                skill_map[label] = value
    meta_data = soup.find('div', {'class': 'meta'}).text.split(' ')
    length = len(meta_data)
    weight = meta_data[length - 1]
    height = meta_data[length - 2].split('\'')[0] + '\'' + meta_data[length - 2].split('\'')[1].split('\"')[0]
    skill_map["Height"] = height
    skill_map['Weight'] = weight
    if ('Position' in skill_map.keys()):
        if skill_map['Position'] in ('', 'RES', 'SUB'):
            skill_map['Position'] = soup.find('article').find('div', {'class': 'meta'}).find('span').text
        if (skill_map['Position'] != 'GK'):
            card_rows = soup.find('aside').find('div', {'class': 'card mb-2'}).find('div',
                                                                                    {'class': 'card-body'}).findAll(
                'div', {'class': 'columns'})
            for c_row in card_rows:
                attributes = c_row.findAll('div', {'class': re.compile('column col-sm-2 text-center')})
                for attribute in attributes:
                    if (attribute.find('div')):
                        name = ''.join(re.findall('[a-zA-Z]', attribute.text))
                        value = attribute.text.replace(name, '').strip()
                        skill_map[str(name)] = value
    all_tables = soup.find('article').findAll('div', {'class': 'columns spacing'})

    first_sections = all_tables[0].findAll('div', {'class': 'column col-4'})
    second_sections = all_tables[1].findAll('div', {'class': 'column col-4'})

    sections = first_sections + second_sections
    for section in sections:
        uls = section.find('ul')
        if uls is None:
            continue
        items = uls.findAll('li')
        for item in items:
            try:
                value = int(re.findall(r'\d+', item.text)[0])
            except IndexError:
                value = 0
            name = ''.join(re.findall('[a-zA-Z]*', item.text))
            skill_map[str(name)] = value

    player_stats = pd.DataFrame(skill_map, index=[date])

    return player_stats


def retrieve_player_stats_by_fifa_update(player_url, player_statistics, fifa_version, update_version):
    """Retrieve the statistics of a player for a particular version

    Parameters
    ----------
    player_url: str
        The base url from which retrieve the data of the target player
    player_statistics: list
        The statistics of the player to retrieve
    fifa_version: int
        The version of FIFA
    update_version: int
        The number of the update of the given FIFA version

    Returns
    -------
    player_statistics_df: pd.DataFrame
        If exist, the DataFrame containing the statistics of the player for the given update. An empty DataFrame is
        returned if no statistics of the player were found for the given fifa_version and update_version
    """
    # The url requires that the update version has two digits
    padded_update_version = str(update_version).zfill(2)
    base_fifa_version_url = f"?r={fifa_version}00{padded_update_version}&set=true"
    url = player_url + base_fifa_version_url
    plain_text = requests.get(url).text

    if not _source_update_version_is_valid(plain_text, fifa_version):
        return pd.DataFrame()

    date = _parse_fifa_update_date(plain_text)
    player_stats = _extract_stats(plain_text, player_statistics, date)

    return player_stats


def retrieve_player_stats_by_fifa_version(base_url, player_id, player_statistics, fifa_version):
    """Retrieve the statistics of a player for a given FIFA version, considering all the updates occurred in that
    given version

    Parameters
    ----------
    base_url: str
        The url to which make the request
    player_id: int
        The id of the player
    player_statistics: list
        The statistics of the player to retrieve
    fifa_version: int
        The version of FIFA from which retrieve the statistics

    Returns
    -------
    player_statistics: pd.DataFrame
        A DataFrame containing all the statistics of the player, where each row is the set of statistics for a
        particular update
    """
    all_player_stats = []

    for update_version in range(1, LAST_UPDATE_PER_FIFA_VERSION[fifa_version] + 1):
        player_url = base_url + str(player_id)
        all_player_stats.append(retrieve_player_stats_by_fifa_update(player_url,
                                                                     player_statistics,
                                                                     fifa_version,
                                                                     update_version))

    return pd.concat(all_player_stats, sort=True)


class HistoricalScraper:
    def __init__(self, player_attributes_url, player_statistics_url, player_per_request, number_of_players_pages):
        self.number_of_players_pages = number_of_players_pages
        self.player_per_request = player_per_request
        self.player_attributes_url = player_attributes_url
        self.player_statistics_url = player_statistics_url

    def download_all_player_attributes(self, player_attributes):
        """Download the attributes for all the players, according to the number of requests and to the chosen offset

        Returns
        -------
        player_df: pd.DataFrame
            A DataFrame containing the attributes of all the retrieved players
        """
        all_player_attributes_dfs = []

        for offset in range(0, self.number_of_players_pages):
            plain_text = retrieve_raw_player_attributes(self.player_attributes_url, offset * self.player_per_request)
            all_player_attributes_dfs.append(extract_player_attributes(plain_text, player_attributes))

        player_attributes_df = pd.concat(all_player_attributes_dfs)
        return player_attributes_df

    def _single_thread_download_historical_player_statistics(self,
                                                             player_ids,
                                                             player_statistics,
                                                             starting_fifa_version,
                                                             ending_fifa_version,
                                                             save=True):
        all_players_size_index = 0
        all_players_size = len(player_ids) - 1
        all_player_stats = []

        for player_id in player_ids:
            print(f"Player {all_players_size_index} of {all_players_size}")
            for fifa_version in range(starting_fifa_version, ending_fifa_version + 1):
                player_stats_for_version = retrieve_player_stats_by_fifa_version(base_url=self.player_statistics_url,
                                                                                 player_id=player_id,
                                                                                 player_statistics=player_statistics,
                                                                                 fifa_version=fifa_version)
                player_stats_for_version['player_id'] = [player_id for i in range(len(player_stats_for_version))]
                print(player_stats_for_version)
                all_player_stats.append(player_stats_for_version)
            all_players_size_index += 1

        all_player_stats_df = pd.concat(all_player_stats, sort=True)

        if save:
            first_id = player_ids[0]
            last_id = player_ids[-1]
            all_player_stats_df.to_parquet(f"player_stats_"
                                           f"{starting_fifa_version}_"
                                           f"{ending_fifa_version}_"
                                           f"{first_id}_"
                                           f"{last_id}.parquet")

        return all_player_stats_df

    def download_historical_player_statistics(self,
                                              player_ids,
                                              player_statistics,
                                              starting_fifa_version,
                                              ending_fifa_version,
                                              n_threads=1):
        """Download the player_statistics for all the player ids, starting from the starting_fifa_version to the
        ending_fifa_version.

        Parameters
        ----------
        player_ids: List
            The ids of all the players
        player_statistics: List
            The statistics to retrieve for each player
        starting_fifa_version: int
            Year of the starting version of FIFA
        ending_fifa_version: int
            Year of the ending version of FIFA

        Returns
        -------
        player_statistics_df: pd.DataFrame:
            The DataFrame containing the player_statistics for all the requested players
        """
        player_ids_split = _compute_id_splits(player_ids, n_threads)

        threading_function_args = [(player_ids_chunck,
                                    player_statistics,
                                    starting_fifa_version,
                                    ending_fifa_version) for player_ids_chunck in player_ids_split]

        with Pool(n_threads) as pool:
            player_stats = pool.starmap(self._single_thread_download_historical_player_statistics,
                                        threading_function_args)

        all_player_stats_df = pd.concat(player_stats, ignore_index=True)

        return all_player_stats_df
