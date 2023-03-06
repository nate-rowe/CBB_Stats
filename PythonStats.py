import pandas as pd
import pygsheets
import requests
import datetime
from bs4 import BeautifulSoup


pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', None)  # show full width of showing cols
pd.set_option("expand_frame_repr", False)  # print cols side by side as it's supposed to be

year = datetime.date.today().year

# Google Sheets authentication
def authenticate():
    print("Authenticating...")

    creds = 'C:/Users/Nate/Desktop/Projects/Python/CBB Stats/civil-pattern-316901-a503a0c5c842.json'
    api = pygsheets.authorize(service_file=creds)
    workbook = api.open('PythonSheets')

    return workbook


# Scrape web data into pandas dataframe
def load_data(url):
    table = None
    print("Loading from: " + url)

    res = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    soup = BeautifulSoup(res.text, features="html.parser")

    if "kenpom" in url:
        table_html = soup.find_all('table', {'id': 'ratings-table'})
        table = table_html[0]
        thead = table_html[0].find_all('thead')

        # Fix the weird Kenpom thead HTML formatting that is causing issues loading into pandas
        for x in thead:
            table = str(table).replace(str(x),'')  # Remove thead instances

    if "sports-reference" in url:
        table_html = None

        # Find the table by unique ID
        if "advanced-opponent-stats.html" in url:
            table_html = soup.find_all('table', {'id': 'adv_opp_stats'})
        elif "advanced-school-stats.html" in url:
            table_html = soup.find_all('table', {'id': 'adv_school_stats'})
        elif ("school-stats.html" in url) and (url[-19:][0].isdigit()):  # Digit in this position is what makes the url unique
            table_html = soup.find_all('table', {'id': 'basic_school_stats'})
        elif ("opponent-stats.html" in url) and (url[-21:][0].isdigit()):  # Digit in this position is what makes the url unique
            table_html = soup.find_all('table', {'id': 'basic_opp_stats'})

        table = table_html[0]

        over_under_thead = table.find_all('tr', {'class': 'over_header thead'})
        thead = table.find_all('tr', {'class': 'thead'})
        thead2 = table.find_all('thead')
        caption = table.find_all('caption')
        colgroup = table.find_all('colgroup')

        # Remove all instances of unnecessary HTML tags found in the table
        for x in over_under_thead:
            table = str(table).replace(str(x),'')

        for x in thead:
            table = str(table).replace(str(x),'')

        for x in thead2:
            table = str(table).replace(str(x),'')

        for x in caption:
            table = str(table).replace(str(x),'')

        for x in colgroup:
            table = str(table).replace(str(x),'')

    dataframe = pd.read_html(table)[0]

    return dataframe


def clean_dataframe(url,dataframe):
    if "kenpom" in url:
        # Rename dataframe columns
        dataframe.columns = ['Rank', 'Team', 'Conference', 'W-L', 'Adj Efficiency Margin', 'Adj Off Rating', 'Adj Off Rank',
                      'Adj Def Rating', 'Adj Def Rank', 'Adj Tempo', 'Adj Tempo Rank', 'Luck', 'Luck Rank',
                      'Opp Adj Efficiency Margin', 'Opp Adj Efficiency Margin Rank', 'Opp Off Efficiency',
                      'Opp Off Efficiency Rank', 'Opp Def Efficiency', 'Opp Def Efficiency Rank',
                      'Non-Con Adj Efficiency',
                      'Non-Con Adj Efficiency Rank']

        # Some team names are formatted differently between the different websites. Reformat the Kenpom team names so that
        # they match the team names from the sports-reference pages.
        dataframe['Team'].replace('St\.', 'State', regex=True, inplace=True)
        dataframe['Team'].replace('BYU', 'Brigham Young', regex=True, inplace=True)
        dataframe['Team'].replace('Bowling Green', 'Bowling Green State', regex=True, inplace=True)
        dataframe['Team'].replace('Cal Baptist', 'California Baptist', regex=True, inplace=True)
        dataframe['Team'].replace('Central Connecticut', 'Central Connecticut State', regex=True, inplace=True)
        dataframe['Team'].replace('Charleston', 'College of Charleston', regex=True, inplace=True)
        dataframe['Team'].replace('FIU', 'Florida International', regex=True, inplace=True)
        dataframe['Team'].replace('Grambling State', 'Grambling', regex=True, inplace=True)
        dataframe['Team'].replace('LIU', 'Long Island University', regex=True, inplace=True)
        dataframe['Team'].replace('LSU', 'Louisiana State', regex=True, inplace=True)
        dataframe['Team'].replace('UCF', 'Central Florida', regex=True, inplace=True)
        dataframe['Team'].replace('College of Charleston Southern', 'Charleston Southern', regex=True, inplace=True)
        dataframe['Team'].replace('UMass Lowell', 'Massachusetts Lowell', regex=True, inplace=True)
        dataframe['Team'].replace("Mount State Mary's", "Mount St. Mary's", regex=True, inplace=True)
        dataframe['Team'].replace("N.C. State", "NC State", regex=True, inplace=True)
        dataframe['Team'].replace("Nebraska Omaha", "Omaha", regex=True, inplace=True)
        dataframe['Team'].replace("Penn", "Pennsylvania", regex=True, inplace=True)
        dataframe['Team'].replace("Prairie View A&M", "Prairie View", regex=True, inplace=True)
        dataframe['Team'].replace("State Francis NY", "Saint Francis NY", regex=True, inplace=True)
        dataframe['Team'].replace("State Francis PA", "Saint Francis PA", regex=True, inplace=True)
        dataframe['Team'].replace("SMU", "Southern Methodist", regex=True, inplace=True)
        dataframe['Team'].replace("USC Upstate", "South Carolina Upstate", regex=True, inplace=True)
        dataframe['Team'].replace("USC", "Southern California", regex=True, inplace=True)
        dataframe['Team'].replace("Southern Miss", "Southern Mississippi", regex=True, inplace=True)
        dataframe['Team'].replace("State Bonaventure", "St. Bonaventure", regex=True, inplace=True)
        dataframe['Team'].replace("State Francis NY", "Saint Francis NY", regex=True, inplace=True)
        dataframe['Team'].replace("State Francis PA", "Saint Francis PA", regex=True, inplace=True)
        dataframe['Team'].replace("State Thomas", "St. Thomas", regex=True, inplace=True)
        dataframe['Team'].replace("Texas A&M Corpus Chris", "Texas A&M Corpus Christi", regex=True, inplace=True)
        dataframe['Team'].replace("UT Rio Grande Valley", "Texas Rio Grande Valley", regex=True, inplace=True)
        dataframe['Team'].replace("UMBC", "Maryland Baltimore County", regex=True, inplace=True)
        dataframe['Team'].replace("UMKC", "Kansas City", regex=True, inplace=True)
        dataframe['Team'].replace("UNLV", "Nevada Las Vegas", regex=True, inplace=True)
        dataframe['Team'].replace("VCU", "Virginia Commonwealth", regex=True, inplace=True)
        dataframe['Team'].replace("Pennsylvania State", "Penn State", regex=True, inplace=True)

    if "sports-reference" in url:
        if "advanced-school-stats" in url:
            for col in dataframe.columns:  # Remove the empty columns in the dataframe
                if col in (0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20):
                    dataframe.drop(col, axis=1, inplace=True)

            # Rename columns
            dataframe.columns = ['Team', 'Pace', 'Off Rating', 'Free Throw Rate', '3 Pt Rate', 'True Shooting %',
                                 'Rebounding %', 'Assist %', 'Steal %', 'Block %', 'Effective FG %', 'Turnover %',
                                 'Off Rebound %', 'FT/FGA']
        elif "advanced-opponent-stats" in url:
            # Remove the empty columns
            # Also remove columns are the duplicates of columns in "advanced-school-stats"
            for col in dataframe.columns:
                if col in (0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21):
                    dataframe.drop(col, axis=1, inplace=True)

            # Rename columns
            dataframe.columns = ['Team', 'Off Rating', 'Free Throw Rate', '3 Pt Rate', 'True Shooting %',
                                 'Rebounding %', 'Assist %', 'Steal %', 'Block %', 'Effective FG %', 'Turnover %',
                                 'Off Rebound %', 'FT/FGA']
        elif ("school-stats.html" in url) and (url[-19:][0].isdigit()):
            # Remove empty and unneccesary columns
            for col in dataframe.columns:
                if col in (0,8,11,14,17,20):
                    dataframe.drop(col, axis=1, inplace=True)

            # Rename columns
            dataframe.columns = ['Team', 'Games', 'Wins', 'Losses', 'Win %', 'Simple Rating System',
                                 'Strength of Schedule', 'Conf Wins', 'Conf Losses', 'Home Wins', 'Home Losses',
                                 'Away Wins', 'Away Losses', 'Points Scored', 'Points against', 'Minutes Played',
                                 'FG Made', 'FG Attempted', 'FG %', '3Pt Made', '3Pt Attempted', '3Pt %', 'FT Made',
                                 'FT Attempted', 'FT %', 'Off Rebounds', 'Total Rebounds','Assists', 'Steals', 'Blocks',
                                 'Turnovers', 'Fouls']
        elif ("opponent-stats.html" in url) and (url[-21:][0].isdigit()):
            # Remove empty and unneccesary columns
            for col in dataframe.columns:
                if col in (0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21):
                    dataframe.drop(col, axis=1, inplace=True)

            # Rename columns
            dataframe.columns = ['Team', 'FG Made', 'FG Attempted', 'FG %', '3Pt Made', '3Pt Attempted','3Pt %',
                                 'FT Made', 'FT Attempted', 'FT %', 'Off Rebounds', 'Total Rebounds', 'Assists',
                                 'Steals', 'Blocks', 'Turnovers', 'Fouls']

        # Replace the state abbreviations with the full state name for schools that are in multiple states
        dataframe["Team"].replace("Loyola (IL)", "Loyola Chicago", inplace=True)
        dataframe['Team'].replace('Loyola (MD)', 'Loyola MD', inplace=True)
        dataframe["Team"].replace("Miami (FL)", "Miami FL", inplace=True)
        dataframe["Team"].replace("Miami (OH)", "Miami OH", inplace=True)
        dataframe["Team"].replace("St. Francis (NY)", "Saint Francis NY", inplace=True)
        dataframe["Team"].replace("Saint Francis (PA)", "Saint Francis PA", inplace=True)

        # Remove the state abbreviations that sports-reference appends to some team names
        dataframe['Team'].replace(r'\([A-Z][A-Z]\)', '', regex=True, inplace=True)

        # Remove the - that sports-reference adds to some team names
        dataframe['Team'].replace(r'-', ' ', regex=True, inplace=True)

    return dataframe


def add_to_sheet(sheet_name, dataframe):
    sheet = wb.worksheet_by_title(sheet_name)
    sheet.clear()
    sheet.set_dataframe(dataframe, (1, 1))


# Dictionary to hold the stats site (key) and the corresponding Google Sheet name (value)
dict_urls = {"https://www.kenpom.com": "KenPom",
             "https://www.sports-reference.com/cbb/seasons/2023-advanced-school-stats.html": "SR_Adv_Stats",
             "https://www.sports-reference.com/cbb/seasons/2023-advanced-opponent-stats.html": "SR_Adv_Opp_Stats",
             "https://www.sports-reference.com/cbb/seasons/2023-school-stats.html": "SR_Basic_Stats",
             "https://www.sports-reference.com/cbb/seasons/2023-opponent-stats.html": "SR_Basic_Opp_Stats"}

# Start Google Sheets authentication
wb = authenticate()

for url, worksheet in dict_urls.items():
    df = load_data(url)
    clean_dataframe(url, df)
    add_to_sheet(worksheet, df)

print("Complete!")