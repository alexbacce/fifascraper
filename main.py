import glob
from scraper.historical_scraper import HistoricalScraper
import pandas as pd
import numpy as np

NUMBER_OF_PLAYER_PAGES = 350
PLAYERS_PER_REQUEST = 60

starting_fifa_version = 20
ending_fifa_version = 20
n_threads = 4

general_player_attributes_base_file = "player_attributes.parquet"
historical_player_statistics_file = f"player_stats{starting_fifa_version}_{ending_fifa_version}.parquet"
force_overwrite = False

PLAYER_ATTRIBUTES_URL = "https://sofifa.com/players?offset="
PLAYER_STATISTICS_URL = "https://sofifa.com/player/"
PLAYER_ATTRIBUTES = ['ID', 'Name', 'Nationality', 'Overall', 'Potential', 'Club', 'Value']


PLAYER_STATISTICS = ['Position', 'Joined', 'Loaned From', 'Crossing', 'Finishing', 'HeadingAccuracy', 'ShortPassing',
                     'Volleys', 'Dribbling', 'Curve',
                     'FKAccuracy', 'LongPassing', 'BallControl', 'Acceleration', 'SprintSpeed', 'Agility',
                     'Reactions', 'Balance', 'ShotPower', 'Jumping', 'Stamina', 'Strength', 'LongShots',
                     'Aggression', 'Positioning', 'Vision',
                     'Marking', 'GKDiving', 'GKHandling', 'GKKicking',
                     'GKPositioning', 'GKReflexes']


def main():
    scraper = HistoricalScraper(player_attributes_url=PLAYER_ATTRIBUTES_URL,
                                player_statistics_url=PLAYER_STATISTICS_URL,
                                number_of_players_pages=NUMBER_OF_PLAYER_PAGES,
                                player_per_request=PLAYERS_PER_REQUEST)

    player_attributes_files = glob.glob("./*" + general_player_attributes_base_file + "*")

    if len(player_attributes_files) == 0 or force_overwrite:
        player_df = scraper.download_all_player_attributes(player_attributes=PLAYER_ATTRIBUTES,
                                                           output_file=general_player_attributes_base_file)
    else:
        player_df = pd.read_parquet(player_attributes_files[0])

    player_ids = np.unique(player_df['ID'].values)

    player_stats_files = glob.glob("./*" + historical_player_statistics_file + "*")

    if len(player_stats_files) == 0 or force_overwrite:
        partial_player_stats = pd.read_parquet("data/player_stats_20_20.parquet")
        players_ids_to_compute = np.setdiff1d(player_ids, partial_player_stats['player_api_id'].values)
        all_player_stats_df = scraper.download_historical_player_statistics(player_ids=players_ids_to_compute,
                                                                            player_statistics=PLAYER_STATISTICS,
                                                                            starting_fifa_version=starting_fifa_version,
                                                                            ending_fifa_version=ending_fifa_version,
                                                                            n_threads=n_threads)
        all_player_stats_df.to_parquet(f"player_stats_{starting_fifa_version}_{ending_fifa_version}.parquet", index=True)


if __name__ == "__main__":
    main()
