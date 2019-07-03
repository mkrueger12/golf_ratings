import pandas as pd
import numpy as np
import sys
import pickle
import datetime

from helpers import *
from settings import *

sys.path.insert(0, './classes')
from player import Player
from glicko import Glicko
from elo import Elo

MU = glicko_set['init']
ratio = glicko_set['ratio']

df = pd.read_csv('./data/3M.csv')
pr = pd.read_csv('./data/current_player_ratings.csv')

field = list(df.Name.unique())

# field player ratings
fpr = pr.loc[pr['name'].isin(field)]

def get_expected(pairs):

    p1_name = pairs[0]
    p2_name = pairs[1]

    if p2_name=='Charlie Hoffman':
        p2_name = 'Charley Hoffman'

    p1_row = fpr.loc[pr['name']==p1_name]
    print(p1_row)

    p2_row = fpr.loc[pr['name']==p2_name]
    print(p2_row)


    p1_elo = p1_row['ielo'].values[0]
    p2_elo = p2_row['ielo'].values[0]

    elo_expected = Elo.x(p1_elo,p2_elo)

    impact = Glicko.reduce_impact(p2_row['gvar'])
    mu = (p1_row['glicko'].values[0] - MU)/ratio
    opp_mu = (p2_row['glicko'].values[0] - MU)/ratio
    glicko_expected = Glicko.get_expected(mu, opp_mu, impact)

    p1_asg = p1_row['asg'].values[0]
    p1_var = p1_row['pvar'].values[0]
    p2_asg = p2_row['asg'].values[0]
    p2_var = p2_row['pvar'].values[0]
    sg_x = asg_pred(p1_asg,p1_var,p2_asg,p2_var)

    p1_ldate = p1_row['last_date'].values[0]
    p1_ldate = datetime.datetime.strptime(str(p1_ldate), '%b %d %Y').date()
    current_date = datetime.datetime.strptime(str('Jul 4 2019'), '%b %d %Y').date()
    p1_dsl = current_date - p1_ldate
    p1_days = p1_dsl.days

    p2_ldate = p2_row['last_date'].values[0]
    p2_ldate = datetime.datetime.strptime(str(p2_ldate), '%b %d %Y').date()
    p2_dsl = current_date - p2_ldate
    p2_days = p2_dsl.days

    p1_rnds_played = p1_row['rnds_played'].values[0]
    p2_rnds_played = p2_row['rnds_played'].values[0]

    row = [elo_expected, sg_x, glicko_expected, 2, p1_days,p1_rnds_played,p2_days,p2_rnds_played]
    return row

pairs = [
['Charles Howell III','Joaquin Niemann'],
['Viktor Hovland', 'Rory Sabbatini'],

]

Elo = Elo()
Glicko = Glicko()
model = pickle.load(open('model.sav', 'rb'))

all_expected = []
for pair in pairs:
    expected = np.array(get_expected(pair))
    prediction = model.predict_proba(expected.reshape(1,-1))[0][0]
    print(prediction)
    p1_name = pair[0]
    p2_name = pair[1]
    all_expected.append([p1_name,p2_name,prediction])

mdf = pd.DataFrame(all_expected, columns=['Player 1', 'Player 2','Model'])

def pct_to_odds(x):
    x *= 100
    if x >= 50:
        y = 0 - (x/(100 - x)) * 100
    else:
        y = ((100-x)/x) * 100
        y = '+' + str(np.round(y,3))
        # 53 / 47
    return y
mdf['Line P1'] = mdf['Model'].apply(lambda x: pct_to_odds(x))
mdf['Line P2'] = mdf['Model'].apply(lambda x: pct_to_odds(1-x))

# print(mdf)
print(mdf[['Player 1', 'Player 2', 'Model', 'Line P1', 'Line P2']])
# end
