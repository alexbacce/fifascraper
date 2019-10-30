from multiprocessing.pool import Pool

import pandas as pd

from scraper.historical_scraper import HistoricalScraper

NUMBER_OF_PLAYER_PAGES = 1
PLAYERS_PER_REQUEST = 61

PLAYER_ATTRIBUTES_URL = "https://sofifa.com/players?offset="
PLAYER_STATISTICS_URL = "https://sofifa.com/player/"
PLAYER_ATTRIBUTES = ['ID', 'Name', 'Nationality', 'Overall', 'Potential', 'Club', 'Value']


PLAYER_STATISTICS = ['Preferred Foot', 'International Reputation', 'Weak Foot', 'Skill Moves', 'Work Rate', 'Body Type',
                     'Real Face', 'Position', 'Jersey Number', 'Joined', 'Loaned From', 'Contract Valid Until',
                     'Height', 'Weight', 'LS', 'ST', 'RS', 'LW', 'LF', 'CF', 'RF', 'RW', 'LAM', 'CAM', 'RAM', 'LM',
                     'LCM', 'CM', 'RCM', 'RM', 'LWB', 'LDM', 'CDM', 'RDM', 'RWB', 'LB', 'LCB', 'CB', 'RCB', 'RB',
                     'Crossing', 'Finishing', 'HeadingAccuracy', 'ShortPassing', 'Volleys', 'Dribbling', 'Curve',
                     'FKAccuracy', 'LongPassing', 'BallControl', 'Acceleration', 'SprintSpeed', 'Agility',
                     'Reactions', 'Balance', 'ShotPower', 'Jumping', 'Stamina', 'Strength', 'LongShots',
                     'Aggression', 'Interceptions', 'Positioning', 'Vision', 'Penalties', 'Composure',
                     'Marking', 'StandingTackle', 'SlidingTackle', 'GKDiving', 'GKHandling', 'GKKicking',
                     'GKPositioning', 'GKReflexes', 'ID']


def main():
    scraper = HistoricalScraper(player_attributes_url=PLAYER_ATTRIBUTES_URL,
                                player_statistics_url=PLAYER_STATISTICS_URL,
                                number_of_players_pages=NUMBER_OF_PLAYER_PAGES,
                                player_per_request=PLAYERS_PER_REQUEST)

    player_df = scraper.download_all_player_attributes(player_attributes=PLAYER_ATTRIBUTES)
    player_ids = player_df['ID'].values
    all_player_stats_df = scraper.download_historical_player_statistics(player_ids=player_ids,
                                                                        player_statistics=PLAYER_STATISTICS,
                                                                        starting_fifa_version=20,
                                                                        ending_fifa_version=20,
                                                                        n_threads=4)

    all_player_stats_df.to_parquet("player_stats.parquet")


if __name__ == "__main__":
    main()
