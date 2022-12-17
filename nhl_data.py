# Import all dependencies
import requests
import pandas as pd
from pandasql import sqldf
from datetime import date
from datetime import datetime

def seasonalRoster(teamID, season):
    """Request a team's roster for a given season
    Args:
        teamID (int): id of team you want data for.
        season (int): year that the desired season started. Ex: The 2014-2015 season would use input 2014.
    Returns:
        list: list of each player's info on the team for that season
    """
    params = {"expand": "team.roster", "season": f"{season}{season+1}"}
    response = requests.get(f"https://statsapi.web.nhl.com/api/v1/teams/{teamID}", params=params).json()
    roster = response['teams'][0]['roster']['roster']

    return roster


def query_player_personal_info(link, season):
    """Request a players personal information
    Args:
        link (str): endpoint used for the API to request a specific player. Ex: '/api/v1/people/ID' where ID is unique to each player
        season (int): year that the desired season started. Ex: The 2014-2015 season would use input 2014.
    Returns:
        dictionary of a players personal infomation. Keys are ['id', 'fullName', 'height', 'weight', 'age']
    """
    
    playerInfoResponse = requests.get(f"https://statsapi.web.nhl.com{link}").json()
    
    # Calculate age of player
    player_birtdate = playerInfoResponse['people'][0]['birthDate']
    age_of_player = calculate_age_of_player_during_season(player_birtdate, season)
    
    
    # filter for the keys we want to keep
    keys = ['id', 'fullName', 'height', 'weight']
    filtered_player_personal_info = {x:playerInfoResponse['people'][0][x] for x in keys}
    
    # Add age into data we return
    filtered_player_personal_info['age'] = age_of_player

    return filtered_player_personal_info


def query_player_seasonal_stats(link, season):
    """Request a players season stats
    Args:
        link (str): endpoint used for the API to request a specific player. Ex: '/api/v1/people/ID' where ID is unique to each player
        season (int): year that the desired season started. Ex: The 2014-2015 season would use input 2014.
    Returns:
        dict: dict of a player's stats for the specified season.
    """
    playerStatsResponse = requests.get(f"https://statsapi.web.nhl.com{link}/stats?stats=statsSingleSeason&season={season}{season+1}").json()
    
    return playerStatsResponse['stats'][0]['splits'][0]['stat']


def calculate_age_of_player_during_season(birthday, season):
    """Calculate the age of a player
    Args:
        birthday (string): Birthday of player. Format must be "YYYY-MM-DD" with zero-padding on numbers less than 10.
        season (int): year that the desired season started. Ex: The 2014-2015 season would use input 2014.
    Returns:
        string of a player's age in the form "Years-Days"
        
    **NOTE: This age is the players age in Years-Days as of Jan 31st of that season. Ex: For the 2014-15 season it would be as of Jan 31, 2015
    """
    # Cutoff date to calculate age is Jan 31 of that season according to https://www.hockey-reference.com/about/glossary.html
    cutoff_date = date(season+1, 1, 31)
    # Convert players birthday to a datetime object
    player_birthday = datetime.strptime(birthday, '%Y-%m-%d').date()
    
    # Calculate players age in days
    days = (cutoff_date - player_birthday).days
    
    # Initialize counter for age in years
    years = 0
    # Remove 365 days from total age in days and increase years count by 1 each time.
    # Stop when the age in days is less than a year.
    while days > 365:
        years += 1
        days -= 365
    
    # Account for leap years
    number_of_leap_years = round((cutoff_date.year - player_birthday.year)/4)
    days -= number_of_leap_years
    
    # If subtracting leap years from days causes it to be negative, subtract 1 from years and remove that many days from 365
    if (days < 0):
        years -= 1
        days = 365 + days
    
    return f"{years}-{days}"


def reorder_columns(dataframe, col_name, position):
    """Reorder a dataframe's column.
    Args:
        dataframe (pd.DataFrame): dataframe to use
        col_name (string): column name to move
        position (0-indexed position): where to relocate column to
    Returns:
        pd.DataFrame: re-assigned dataframe
    """
    temp_col = dataframe[col_name]
    dataframe = dataframe.drop(columns=[col_name])
    dataframe.insert(loc=position, column=col_name, value=temp_col)
    return dataframe


def create_panthers_players_table():
    """Create DataFrame of stats for each player on a season's roster between two seasons
    Args:
        firstSeason (int or string): earliest season we want data from
        secondSeason (int or string): most recent season we want data from
        teamID (int or string): id of team in we want data from
    Returns:
        pd.DataFrame: dataframe of individual player's info and stats for each season between firstSeason and lastSeason
        
    ***Note: the arguments should be the year the season began. Ex: if the first season we want data from is the 
        2014-2015 season, then the argument would be "2014". The same rules apply for the lastSeason argument.
    """
    
    # Initialize empty dataframe
    panthers_players_table = pd.DataFrame()
    
    # Loop through each team
    teams = requests.get("https://statsapi.web.nhl.com/api/v1/teams/").json()['teams']
    for team in teams:
        teamID = int(team['id'])
        firstSeason = int(team['firstYearOfPlay'])
    
        # Loop through each season and get that season's roster
        for season in range(firstSeason, 2021):
            print(team['name'], season)
            try:
                roster_for_season = seasonalRoster(teamID, season)

                # For each player on the season's roster, loop through and query their info and stats
                for player in roster_for_season:
                    # Try to gather data for player. If no data is present, then skip to the next player
                    try:
                        personal_info = query_player_personal_info(player['person']['link'], season)
                        player_seasonal_stats = query_player_seasonal_stats(player['person']['link'], season)
                        
                        print(personal_info['fullName'])
                        
                        # Combine two dicts of info into one dict to appends to df
                        final_row_of_info = {**personal_info, **player_seasonal_stats}
                        # Add season as a key-value pair
                        final_row_of_info['season'] = f"{season}-{season+1}"
                        # Append this players info to the main dataframe
                        panthers_players_table = panthers_players_table.append(final_row_of_info, ignore_index = True)
                    except:
                        continue
            except: 
                continue
    return panthers_players_table


if __name__ == "__main__":
    # Create the dataframe with the desired information
    panthers_players_table = create_panthers_players_table()

    # Move "season" column to second position
    panthers_players_table = reorder_columns(dataframe=panthers_players_table, col_name='season', position=1)

    # Write the dataframe to CSV file
    panthers_players_table.to_csv("panthers_players_table.csv", index=False)

