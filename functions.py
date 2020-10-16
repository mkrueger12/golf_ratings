import pandas as pd
import boto3
from io import StringIO
import time
import json
import requests
import datetime
from draft_kings.data import Sport
from draft_kings.client import contests



def historicalstatscrape(year, cat):

    time.sleep(10)
    url = f'https://www.pro-football-reference.com/years/{year}/{cat}.htm'
    print(year, cat)
    df = pd.read_html(url)[0]

    return df


def writeToS3(data, bucket_name, filename, bucket_folder):
    s3 = boto3.resource('s3')
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3.Object(bucket_name, f'{bucket_folder}/{filename}').put(Body=csv_buffer.getvalue())


def s3readcsv(bucket_name, bucket_folder, filename):

    key = f'{bucket_folder}/{filename}'
    print(key)
    client = boto3.client('s3')  # low-level functional API

    resource = boto3.resource('s3')  # high-level object-oriented API
    my_bucket = resource.Bucket(bucket_name)  # subsitute this for your s3 bucket name.
    obj = client.get_object(Bucket=bucket_name, Key=key)
    data = pd.read_csv(obj['Body'])
    return data


def weekByWeekHist(year, cat, pages):

    if cat == 'passing':
        url = f'https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={year}&year_max={year}&season_start=1&season_end=-1&age_min=0&age_max=99&game_type=A&league_id=&team_id=&opp_id=&game_num_min=0&game_num_max=99&week_num_min=1&week_num_max=17&game_day_of_week=&game_location=&game_result=&handedness=&is_active=&is_hof=&c1stat=pass_att&c1comp=gt&c1val=1&c2stat=&c2comp=gt&c2val=&c3stat=&c3comp=gt&c3val=&c4stat=&c4comp=gt&c4val=&order_by=pass_rating&from_link=1&offset={pages}00'
    elif cat == 'rushing':
        url = f'https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={year}&year_max={year}&season_start=1&season_end=-1&age_min=0&age_max=99&game_type=A&league_id=&team_id=&opp_id=&game_num_min=0&game_num_max=99&week_num_min=1&week_num_max=17&game_day_of_week=&game_location=&game_result=&handedness=&is_active=&is_hof=&c1stat=rush_att&c1comp=gt&c1val=3&c2stat=&c2comp=gt&c2val=&c3stat=&c3comp=gt&c3val=&c4stat=&c4comp=gt&c4val=&order_by=rush_yds&from_link=1&offset={pages}00'
    elif cat == 'receiving':
        url = f'https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={year}&year_max={year}&season_start=1&season_end=-1&age_min=0&age_max=99&game_type=A&league_id=&team_id=&opp_id=&game_num_min=0&game_num_max=99&week_num_min=1&week_num_max=1&game_day_of_week=&game_location=&game_result=&handedness=&is_active=&is_hof=&c1stat=rec&c1comp=gt&c1val=2&c2stat=&c2comp=gt&c2val=&c3stat=&c3comp=gt&c3val=&c4stat=&c4comp=gt&c4val=&order_by=rec_yds&from_link=1&offset={pages}00'

    df = pd.read_html(url)[0]

    return df

def contest_url(contest_id):
    return "{API_BASE_URL}{CONTESTS_PATH}{contest_id}".format(
        API_BASE_URL=API_BASE_URL,
        CONTESTS_PATH=CONTESTS_PATH,
        contest_id=contest_id,
    )

def draftables_url(draft_group_id):
    return "{API_BASE_URL}{DRAFTGROUPS_PATH}draftgroups/{draft_group_id}/draftables".format(
        API_BASE_URL=API_BASE_URL,
        DRAFTGROUPS_PATH=DRAFTGROUPS_PATH,
        draft_group_id=draft_group_id
    )

def draft_group_url(draft_group_id):
    return "{API_BASE_URL}{DRAFTGROUPS_PATH}{draft_group_id}".format(
        API_BASE_URL=API_BASE_URL,
        DRAFTGROUPS_PATH=DRAFTGROUPS_PATH,
        draft_group_id=draft_group_id
    )

def get_players(url):
    r = requests.get(url)
    data = json.loads(r.content)
    player_details = data['plrs']
    players = [{'player_id': plr['pid'],
             'first_name': plr['nameF'],
             'last_name': plr['nameL'],
             'nationality': plr['ct'],
             'years_on_tour': [int(x) for x in plr['yrs']]}
            for plr in player_details]
    return players

def parse_tournament_date(date):
    return datetime.datetime.strptime(date[:10], "%Y-%m-%d")


def get_tournaments(min_year, max_year, tour_code):
    tournaments = []
    years = range(min_year, max_year+1)
    for year in years:

        if tour_code == 'r':
            r = requests.get(
                "https://statdata.pgatour.com/historicalschedules/r/{}/historicalschedule.json"
                .format(year))
        else:
            r = requests.get(
                "https://statdata.pgatour.com/historicalschedules/h/{}/historicalschedule.json"
                    .format(year))
        data = json.loads(r.content)
        tournaments += [{'tournament_year_id': tournament['ID'],
                         'tournament_id': tournament['PERM_NUM'],
                         'year': int(tournament['YEAR']),
                         'start_date': parse_tournament_date(tournament['START_DATE']),
                         'end_date': parse_tournament_date(tournament['END_DATE']),
                         'tournament_name': tournament['NAME'],
                         'course_id': tournament['COURSE_NUMBER'],
                         'course_name': tournament['COURSE_NAME'],
                         'country': tournament['COUNTRY'],
                         'state': tournament['STATE'],
                         'city': tournament['CITY'],
                         'purse': int(tournament['PURSE']),
                         'major': True if tournament['TRN_TYPE'] == 'MJR' else False}
                        for tournament in data['data']]
    return tournaments


def pga_stat_scrape(stat_id, year):

    base_url = 'https://statdata-api-prod.pgatour.com/api/clientfile/'
    url = base_url + f'YTDEventStats?T_CODE=r&STAT_ID={stat_id}&YEAR={year}&format=json'
    r = requests.get(url)
    r = r.json()
    data = pd.json_normalize(r['tours'])
    years = pd.json_normalize(r['tours'], record_path=['years', 'stats', 'details'],
                              meta=['tourCodeLC', 'tourCodeUC', 'tourName', 'statID'])

    stats = pd.json_normalize(r['tours'][0]['years'][0]['stats'], record_path=['details'],
                              meta=['statID', 'cat', 'rndOrEvt', 'statName', 'tourAvg'])

    tourney = pd.json_normalize(r['tours'][0]['years'][0]['lastTrnProc'])

    stat_titles = pd.json_normalize(r['tours'][0]['years'][0]['stats'][0]['statTitles'])
    stat_titles = stat_titles.transpose().reset_index()
    stat_titles['index'] = stat_titles['index'].str.replace('statsTitle', 'statsValue', regex=True)
    stat_titles = stat_titles.iloc[1:]
    stat_titles.rename(columns={'index': 'variable1'}, inplace=True)
    stat_titles['stat_name'] = stat_titles[[0]]
    stat_titles.drop(columns=stat_titles[[0]], inplace=True)

    id_vars = ['plrNum', 'curRank', 'curRankTied', 'prevRank', 'prevRankTied',
               'plrName.last', 'plrName.first', 'plrName.middle', 'plrName.nickname',
               'statID', 'cat', 'rndOrEvt', 'statName', 'tourAvg']

    keys = [c for c in stats if c.startswith('statValues.statValue')]
    melted = pd.melt(stats, id_vars=id_vars, value_vars=keys, value_name='stat')
    melted['variable'] = melted['variable'].str.replace('statValues.', '')
    melted['varNum'] = melted['variable'].str.strip().str[-1]
    melted['variable1'] = 'statTitle'
    melted['variable1'] = melted['variable1'] + melted['varNum']

    # create final df
    final = melted.merge(stat_titles, on='variable1', how='left')
    final['tour'] = years['tourName']
    final.drop(columns={'variable', 'variable1', 'varNum'}, inplace=True)
    final.replace('Avg.', 'avg', inplace=True)

    # fill with tourney data
    tourney_df = []
    for i in range(1, len(final) + 1):
        tourney_df.append(tourney)
    tourney_df = pd.concat(tourney_df)
    tourney_df = tourney_df.reset_index()

    final['trnName'] = tourney_df['trnName']
    final['permNum'] = tourney_df['permNum']
    final['trnNum'] = tourney_df['trnNum']
    final['endDate'] = tourney_df['endDate']

    return final


def player_trn_scrape(player_id):

    r = requests.get(f'https://statdata.pgatour.com/players/{player_id}/r_recap.json')

    if r.status_code == 200:

        r = r.json()

        data = pd.json_normalize(r['plr']['tours'][0]['years'], record_path=['trnDetails'])

        stats = pd.json_normalize(r['plr']['tours'][0]['years'], record_path=['trnDetails', 'profiles', 'stats'])

        rounds = pd.json_normalize(r['plr']['tours'][0]['years'], record_path=['trnDetails', 'scr', 'rounds'])

        plr = pd.json_normalize(r['plr'])
        plr.drop(columns={'tours', 'currentYear'}, inplace=True)

        # build final df
        df_size = int(len(stats) / len(data))

        # build df of event info to concat with stats data
        df = []
        for record in range(0, len(data)):
            dat = data.loc[record, :]

            for i in range(0, df_size):
                dat = pd.Series(dat, index=data.columns)
                df.append(dat)

        event = pd.DataFrame(df, columns=data.columns)
        event = event.reset_index()

        # iterate to build player info df
        df = []
        for i in range(0, len(stats)):
            df.append(plr)

        df1 = pd.concat(df)
        df1 = df1.reset_index()

        # combine event and player info
        stats['trnNum'] = event['trn.trnNum']
        stats['permNum'] = event['trn.permNum']
        stats['endDate'] = event['endDate']
        stats['finish'] = event['finPos.unfmt']
        stats['posLastRound'] = event['scr.posLastRound']
        stats['posLastRound'] = event['scr.posLastRound']
        stats['totalScr'] = event['scr.totalScr']
        stats['relToPar'] = event['scr.relToPar']
        stats['scrAvgUnFmt'] = event['scrAvgUnFmt']
        stats['scrAvg'] = event['scrAvg']
        stats['plrNum'] = df1['plrNum']
        stats['full'] = df1['full']

        # write to s3
        file = 'tournament-stats'
        BUCKET_FOLDER = f'raw-data/{file}'
        writeToS3(data=stats, bucket_name='golfdfs', filename=f'data_{player_id}.csv',
                  bucket_folder=BUCKET_FOLDER)
        print(file, 'data upload complete')

        stats = []

        # add player and tourney info to rounds data
        # build final df
        df_size = int(len(rounds) / len(data))

        # build df of event info to concat with stats data
        df = []
        for record in range(0, len(data)):
            dat = data.loc[record, :]

            for i in range(0, df_size):
                dat = pd.Series(dat, index=data.columns)
                df.append(dat)

        event = pd.DataFrame(df, columns=data.columns)
        event = event.reset_index()

        # iterate to build player info df
        df = []
        for i in range(0, len(rounds)):
            df.append(plr)

        df1 = pd.concat(df)
        df1 = df1.reset_index()

        # build rounds data
        rounds['trnNum'] = event['trn.trnNum']
        rounds['permNum'] = event['trn.permNum']
        rounds['endDate'] = event['endDate']
        rounds['plrNum'] = df1['plrNum']

        # write to s3
        file = 'round-stats'
        BUCKET_FOLDER = f'raw-data/{file}'
        writeToS3(data=rounds, bucket_name='golfdfs', filename=f'data_{player_id}.csv',
                  bucket_folder=BUCKET_FOLDER)
        print(file, 'data upload complete')

        print('Scrape of', player_id, 'complete.')
        time.sleep(5)
    else:
        print('Player is not currently on tour. Cannot Scrape', player_id, '.')


def get_player_stats(player_id):
    r = requests.get("https://statdata.pgatour.com/players/{}/r_recap.json"
                     .format(player_id))
    data = json.loads(r.content)
    player_data = data['plr']['tours'][0]
    assert player_data['tourCodeLC'] == 'r'

    player_data = parse_player_stats(player_data)

    return player_data


def parse_player_stats(player_data):
    player_data = {int(x['year']): x for x in player_data['years']}

    for year in player_data.keys():
        player_data[year] = {
            x['trn']['permNum']: x
            for x in player_data[year]['trnDetails']}

        for tournament in player_data[year].keys():
            player_data[year][tournament]['scr']['rounds'] = {
                int(x['rndNum']): x for x in
                player_data[year][tournament]['scr']['rounds']
            }

            player_data[year][tournament]['stats'] = (
                player_data[year][tournament]['profiles'][0]['stats']
            )

            player_data[year][tournament]['stats'] = {
                x['stid']: x['stValue'] for x in
                player_data[year][tournament]['stats']
            }

    return player_data


def get_field(tour):
    ''' Get the field based on the DraftKings fields. '''

    # urls
    API_BASE_URL = 'https://api.draftkings.com'
    DRAFTGROUPS_PATH = '/draftgroups/v1/'

    # get contests
    contest = contests(sport=Sport.GOLF)
    contest = contest['contests']
    contest = pd.DataFrame.from_dict(contest)
    df = contest.sort_values(by=['name'])

    dgid = df[df['name'].str.contains(tour)]
    dgid = dgid.iloc[0, 2]

    # get draft table
    url = f"{API_BASE_URL}{DRAFTGROUPS_PATH}draftgroups/{dgid}/draftables"

    df = requests.get(url).json()
    df = pd.Series(df)
    df = df['draftables']
    df = pd.DataFrame(df)
    df = df.iloc[:, [0, 3, 4, 5, 6, 7, 8, 9, 10, 25]]
    df = df[df['status'] != 'O']
    df = df.drop_duplicates()
    df.rename(columns={'displayName': 'name'}, inplace=True)

    return df

