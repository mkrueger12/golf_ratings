import os
import re
from itertools import combinations

import awswrangler as wr
from classes.elo import Elo
from tqdm import tqdm

from functions import get_field
from helpers import *
import requests
from classes.player import Player

# tournament info
year = '2021'
tourn_id = '521'
tour_code = 'r'
sim_tourns = 60000
cut_line = 70

# download tournament field
url = f'https://statdata.pgatour.com/{tour_code}/{tourn_id}/{year}/field.json'
r = requests.get(url)
r = r.json()

# download tournament field
field = get_field(tour='PGA')
field = list(field['name'])

# get data
os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'

# download data
sql = ('SELECT * FROM "golf-processed"."adjusted_sg_table_processed_all"')

stroke_data = wr.athena.read_sql_query(sql, database="golf-processed")

stroke_data = stroke_data[stroke_data['full'].isin(field)]

stroke_data['weighted'] = (stroke_data['lt_sg:tot']*.7) + (stroke_data['st_sg:tot']*.3)

# initialize ratings objects
elo = Elo()

# find unique player combinations for elo calc
for tid in stroke_data['trnyearid']:

    df = stroke_data[stroke_data['trnyearid'] == tid]

    for round in range(1, 5):

        df = df[df['round'] == round]
        # track field strength of the round
        field_sg = df['sg:tot'].mean()
        # track number of opponents for Elo K val
        num_opps = len(df) - 1
        # track elo changes for everyone
        change_dict = {}

        combos = [c for c in combinations(df['full'], 2)]

        for combo in combos:
            p1 = combo[0]
            p2 = combo[1]
            p1_score = float(df[df['full'] == combo[0]]['sg:tot'])
            p2_score = float(df[df['full'] == combo[1]]['sg:tot'])
            margin = abs(p2_score - p1_score)

            # make predictions using each system
            elo_x = Elo.x(p1.elo, p2.elo)
