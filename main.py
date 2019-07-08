
import sys
import numpy as np
import pandas as pd
import gc
import re
import datetime
import matplotlib.pyplot as plt
import scipy as sp
import scipy.optimize
import random

from settings import *
from helpers import *
from operator import itemgetter
from itertools import combinations
from sklearn.metrics import log_loss
from tqdm import tqdm

sys.path.insert(0, './classes')
from player import Player
from glicko import Glicko
from elo import Elo
from map import Map

gc.collect()

def get_tournament_leaderboard(row):
    name = row['name']
    name = name.strip()
    name = name.replace(" ","")
    tour = row['tour']
    season = row['season']
    season = str(season)
    tournament_leaderboard_path = '../golf_scraper/leaderboards/'+season+'/'+tour+'/'+name+'.csv'
    return pd.read_csv(tournament_leaderboard_path)

# load player_database
pdf = pd.DataFrame(columns=['name', 'other'])
# change to dictionary format bc it's quicker
pdf = pdf.set_index('name')
pdf = pdf.to_dict('index')

# load schedule
schedule_path = '../golf_scraper/sched.csv'
sdf = pd.read_csv(schedule_path)

# load list of tournaments that were scraped
collected_path = '../golf_scraper/has_saved.csv'
cdf = pd.read_csv(collected_path)
collected_ids = list(cdf.TID.unique())

# only need tournaments that have been scraped
sdf = sdf.loc[sdf['tid'].isin(collected_ids)]

# sort by oldest first
sdf['end_date'] = pd.to_datetime(sdf['end_date'], format='%b %d %Y')
sdf = sdf.sort_values(by='end_date', ascending=True)

del cdf
gc.collect()

# initialize ratings objects
Glicko = Glicko()
Elo = Elo()

# divide sg between golfers who have sample size and not
# 0 means less than 100 rounds


sdf = sdf.reset_index()

# one season at a time
seasons = list(sdf.season.unique())
seasons.sort()

all_sg0_loss = []
all_sg1_loss = []
all_l5_loss = []
# all_relo_loss = []
all_elo_loss = []
all_glicko_loss = []

all_sea_data = []

# CONFIG
CALC_SG = False
CALC_LOG5 = True

# iterate schedule
for season in seasons:
    sea_df = sdf.loc[sdf.season==season]
    sea_df = sea_df.sort_values(by='end_date', ascending=True)

    # sea_sg0_loss = []
    # sea_sg1_loss = []
    sea_l5_loss = []
    # sea_relo_loss = []
    sea_elo_loss = []
    sea_glicko_loss = []

    sea_data = []

    for index, row in tqdm(sea_df.iterrows()):

        # validate tournament, usually just checking if duplicate
        not_valid = validate_tournament(row)
        if not_valid:
            continue

        # tournament start_date
        start_date = str(row['start_date'])
        ## for testing ##
        # # when to begin testing? ##
        # begin_date = 'Jan 01 2016'
        # dt_start = datetime.datetime.strptime(str(start_date), '%b %d %Y').date()
        # if dt_start <= datetime.datetime.strptime(begin_date, '%b %d %Y').date():
        #     continue


        # load tournament leaderboard based on inferred path
        tlb = get_tournament_leaderboard(row)

        season = str(row['season'])

        # list to contain player objects
        plist = []

        # error tracking
        tsg0_err = []
        tsg1_err = []
        tl5_err = []
        # trelo_err =[]
        telo_err = []
        tglicko_err = []

        # possible options to speed up here
        # could try subtracting sets, then multiple key lookup using pydash or itemgetter
        # note: tried itemgetter and pydash and neither were faster (bc they returned errors on missing)

        for i, r in tlb.iterrows():
            player = r['name']

            # name preprocessing
            player = name_pp(player)
            # if in dict, initialize player class with data
            if player in pdf:
                dict = pdf[player]
                PObj = Player(
                    name=player,
                    elo=dict['ielo'],
                    # relo=dict['relo'],
                    rnds_played=dict['rnds_played'],
                    glicko = dict['glicko'],
                    gvar = dict['gvar'],
                    gsig = dict['gsig'],
                    ldate = dict['last_date'],
                    cdate = start_date,
                    pr4 = dict['pr4'],
                    R1 = r['R1'],
                    R2 = r['R2'],
                    R3 = r['R3'],
                    R4 = r['R4'],
                    asg = dict['asg'],
                    prev_sgs = dict['prev_sgs'],
                    pvar = dict['pvar'],
                    wins = dict['wins'],
                    losses = dict['losses'],
                    ties = dict['ties'],
                    wl = dict['wl'],
                    matches=dict['matches']
                )
            # if not in dict, initialize player class with new player settings
            else:
                tour = r['tour']
                if tour == 'PGA':
                    elo = ielo_set['init']
                    glicko = glicko_set['init']
                    prev_sgs = asg_set['init_pga']
                elif tour == 'Euro':
                    elo = euro_init['elo']
                    glicko = euro_init['glicko']
                    prev_sgs = asg_set['init_euro']
                PObj = Player(
                    name=player,
                    tour=r['tour'],
                    elo=elo,
                    # relo=elo,
                    glicko=glicko,
                    ldate=start_date,
                    cdate=start_date,
                    R1 = r['R1'],
                    R2 = r['R2'],
                    R3 = r['R3'],
                    R4 = r['R4'],
                    prev_sgs=prev_sgs,
                )

            plist.append(PObj)

        # separate round cols using regex
        col_text = ' '.join(list(tlb))
        rounds = re.findall(r"(\bR[1-4]{1}\b)", col_text)

        # find unique player combinations for elo calc
        for round in rounds:

            # track field strength of the round
            field_sg = []
            # track all scores to determine SG
            rnd_scores = []
            # throw out any player that doesn't have a round score
            # still keep them to update after tournament
            good_plist = []
            bad_plist = []
            for p in plist:
                ###
                round_score = getattr(p, round)
                include = validate(round_score)
                if include:
                    p.rnds_played += 1
                    field_sg.append(p.asg)
                    rnd_scores.append(round_score)
                    good_plist.append(p)
                else:
                    bad_plist.append(p)

            # couple tournaments only have zeros for rounds and should be skipped
            if len(good_plist) <= 0:
                continue

            if CALC_SG:
                # determine the field strength via average adjusted strokes gained
                field_str = sum(field_sg)/len(field_sg)
                # determine avg score for SG
                avg_score = sum(rnd_scores)/len(rnd_scores)

            # add uncertainty if it's been awhile since they've played
            # if round == 'R1':
            #     for p in good_plist:
            #         if p.days_since is not None:
            #             p.gvar = add_uncertainty(p.gvar, p.days_since)

            # all combinations of players not cut or withdrawn
            combos = [c for c in combinations(good_plist,2)]

            # track number of opponents for Elo K val
            num_opps = len(good_plist) - 1

            # track elo changes for everyone
            change_dict = {}
            rchange_dict = {}

            for p in good_plist:
                rnd_score = getattr(p, round)
                if CALC_SG:
                    SG = -1 * (rnd_score - avg_score) + field_str
                    p.prev_sgs = np.append(p.prev_sgs,np.float(SG))
                    ASG = p.asg
                    sg_err = rmse(ASG,SG)
                    if p.rnds_played >= 100:
                        tsg1_err.append(sg_err)
                    else:
                        tsg0_err.append(sg_err)
                # print(index,round,p.name,ASG,SG,p.asg,len(p.prev_sgs))
                change_dict[p.name] = 0
                rchange_dict[p.name]=0

            for combo in combos:
                p1 = combo[0]
                p2 = combo[1]
                p1_score = getattr(p1, round)
                p2_score = getattr(p2, round)
                margin = abs(p2_score - p1_score)

                # make predictions using each system
                elo_x = Elo.x(p1.elo, p2.elo)
                # relo_x = Elo.x(p1.relo,p2.relo)
                glicko_x = Glicko.x(p1, p2)
                if CALC_SG:
                    sg_x = asg_pred(p1.asg,p1.pvar,p2.asg,p2.pvar)
                if CALC_LOG5:
                    l5_x = get_l5_x(p1.wl,p2.wl)


                if p1_score <= p2_score:
                    p1_change, p2_change = Elo.get_ielo_delta(elo_x, margin, p1, p2, num_opps, round)
                    # used for error tracking
                    if p1_score == p2_score:
                        # p1_rchange = Elo.get_delta(relo_x, True)
                        # p2_rchange = -1*p1_rchange
                        result = 0.5
                        if CALC_LOG5:
                            p1.add_tie()
                            p2.add_tie()
                    else:
                        result = 1
                        # p1_rchange = Elo.get_delta(relo_x, False)
                        # p2_rchange = -1*p1_rchange
                        if CALC_LOG5:
                            p1.add_win()
                            p2.add_loss()
                else:
                    p1_change, p2_change = Elo.get_ielo_delta(elo_x, (-1*margin), p1, p2, num_opps, round)
                    # p2_rchange = Elo.get_delta(1-relo_x, False)
                    # p2_rchange = -1*p1_rchange
                    result = 0
                    if CALC_LOG5:
                        p1.add_loss()
                        p2.add_win()

                # randomize to deal with class imbalance
                # rand = random.random()
                # if rand > 0.5:
                #     sea_data.append([start_date, p1.elo, p2.elo, p1.glicko, p2.glicko, round, p1.days_since, p1.rnds_played, p2.days_since, p2.rnds_played, result])
                # else:
                #     sea_data.append([start_date, p2.elo, p1.elo, p2.glicko, p1.glicko, round, p2.days_since, p2.rnds_played, p1.days_since, p1.rnds_played, 1-result])

                elo_error = cross_entropy(elo_x, result)
                # relo_error = cross_entropy(relo_x,result)
                glicko_error = cross_entropy(glicko_x, result)
                telo_err.append(elo_error)
                # trelo_err.append(relo_error)
                tglicko_err.append(glicko_error)

                if CALC_SG:
                    sgm_error = cross_entropy(sg_x, result)
                    tsgm_err.append(sgm_error)

                if CALC_LOG5:
                    l5_error = cross_entropy(l5_x, result)
                    tl5_err.append(l5_error)


                change_dict[p1.name] += p1_change
                change_dict[p2.name] += p2_change
                rchange_dict[p1.name] += p1_rchange
                rchange_dict[p2.name] += p2_rchange

            # apply changes to player elo & log5 after round
            for p in good_plist:
                p.elo += change_dict[p.name]
                # p.relo += rchange_dict[p.name]
                # calculate new adjusted strokes gained
                if CALC_SG:
                    p.calc_new_asg()
                if CALC_LOG5:
                    p.calc_wl()

            ## Glicko Calc ##
            # create list for player objects with updated ratings after each round
            new_pobjs = []
            for pobj in good_plist:
                # add all opponents and their round scores for glicko calc
                opps = [[p, getattr(p, round)] for p in good_plist if p != pobj]

                results = []
                player_round_score = getattr(pobj, round)
                # append result vs opponent
                for opponent, opp_score in opps:
                    if opp_score == player_round_score:
                        result = 0.5
                    elif opp_score < player_round_score:
                        result = 0
                    else:
                        result = 1
                    results.append([opponent, result])
                # Glicko class edits glicko rating of player
                new_pobj = Glicko.update(pobj, results)
                new_pobjs.append(new_pobj)
            # reset all the player objects with the new ratings
            good_plist = new_pobjs
            # recombine good_plist and bad_plist
            plist = good_plist + bad_plist

        # update dict
        for p in plist:
            ## track if they played round 4 ##
            try:
                r4_score = int(getattr(p, 'R4'))
                p.pr4 = validate(r4_score)
            except:
                p.pr4 = False
            stats = {'asg':p.asg, 'pvar':p.pvar, 'prev_sgs':p.prev_sgs, 'ielo': p.elo, #'relo':p.relo,
            'rnds_played': p.rnds_played, 'glicko': p.glicko, 'gvar': p.gvar, 'gsig':p.gsig, 'last_date': start_date, 'pr4':p.pr4,
            'wins':p.wins,'losses':p.losses,'ties':p.ties,'wl':p.wl,'matches':p.matches}
            pdf[p.name] = stats
        # calculate error
        # if len(tsg0_err) > 0:
        #     tournament_sg0_loss = np.round(sum(tsg0_err)/len(tsg0_err),5)
        #     sea_sg0_loss.append(tournament_sg0_loss)
        # if len(tsg1_err) > 0:
        #     tournament_sg1_loss = np.round(sum(tsg1_err)/len(tsg1_err),5)
        #     sea_sg1_loss.append(tournament_sg1_loss)
        tournament_elo_loss = np.round(sum(telo_err)/len(telo_err),5)
        # tournament_relo_loss = np.round(sum(trelo_err)/len(trelo_err),5)
        tournament_glicko_loss = np.round(sum(tglicko_err)/len(tglicko_err),5)
        tournament_l5_loss = np.round(sum(tl5_err)/len(tl5_err),5)
        sea_elo_loss.append(tournament_elo_loss)
        # sea_relo_loss.append(tournament_relo_loss)
        sea_glicko_loss.append(tournament_glicko_loss)
        sea_l5_loss.append(tournament_l5_loss)

    # sea_sg0_loss = np.round(sum(sea_sg0_loss)/len(sea_sg0_loss),5)
    # sea_sg1_loss = np.round(sum(sea_sg1_loss)/len(sea_sg1_loss),5)
    sea_row=[season]
    if len(sea_elo_loss)>0:
        sea_elo_loss = np.round(sum(sea_elo_loss)/len(sea_elo_loss),5)
        # sea_relo_loss = np.round(sum(sea_relo_loss)/len(sea_relo_loss),5)
        sea_glicko_loss = np.round(sum(sea_glicko_loss)/len(sea_glicko_loss),5)
        print("Season Elo Loss: ", sea_elo_loss)
        # print("Season rElo Loss: ", sea_relo_loss)
        print("Season Glicko Loss: ", sea_glicko_loss)
        all_elo_loss.append(sea_elo_loss)
        # all_relo_loss.append(sea_relo_loss)
        all_glicko_loss.append(sea_glicko_loss)
        sea_row.append(sea_elo_loss)
        # sea_row.append(sea_relo_loss)
        sea_row.append(sea_glicko_loss)
    if len(sea_l5_loss)>0:
        sea_l5_loss = np.round(sum(sea_l5_loss)/len(sea_l5_loss),5)
        print("Season Log 5 Loss: ",sea_l5_loss)
        all_l5_loss.append(sea_l5_loss)
        sea_row.append(sea_l5_loss)
        # WILL NEED TO MOVE
    if len(sea_row) > 1:
        all_sea_data.append(sea_row)
    print(season)
    # print(sea_sg0_loss)
    # print(sea_sg1_loss)
    # all_sg0_loss.append(sea_sg0_loss)
    # all_sg1_loss.append(sea_sg1_loss)

    # print("Saving data for train...")
    # sea_data_df = pd.DataFrame(sea_data,columns=['Start_Date','P1 Elo', 'P2 Elo', 'P1 Glicko', 'P2 Glicko', 'Round', 'P1_DS', 'P1_RP', 'P2_DS', 'P2_RP','Result'])
    # sea_data_df.to_csv('./data/seasons/'+str(season)+'.csv')


player_ratings = pd.DataFrame.from_dict(pdf, orient='index')
player_ratings.index.name=('name')
player_ratings = player_ratings.reset_index('name')
# player_ratings.prev_sgs = player_ratings.prev_sgs.apply(lambda x: clean_ps(x))
player_ratings = player_ratings.drop(columns=['prev_sgs'])
# player_ratings = player_ratings.loc[player_ratings['rnds_played']>100]
player_ratings = player_ratings.sort_values(by='glicko',ascending=False)
print(player_ratings.head(50))
player_ratings = player_ratings.sort_values(by='ielo',ascending=False)
print(player_ratings.head(50))
player_ratings = player_ratings.sort_values(by='wl',ascending=False)
print(player_ratings.head(50))

player_ratings.to_csv('./data/current_player_ratings.csv',index=False)

# print('TOTAL AVERAGE ASG0 LOSS', str(np.round(sum(all_sg0_loss)/len(all_sg0_loss),5)))
# print('TOTAL AVERAGE ASG1 LOSS', str(np.round(sum(all_sg1_loss)/len(all_sg1_loss),5)))
print('TOTAL AVERAGE L5 LOSS', str(np.round(sum(all_l5_loss)/len(all_l5_loss),5)))
print('TOTAL AVERAGE ELO LOSS', str(np.round(sum(all_elo_loss)/len(all_elo_loss),5)))
# print('TOTAL AVERAGE rELO LOSS', str(np.round(sum(all_relo_loss)/len(all_relo_loss),5)))
print('TOTAL AVERAGE GLICKO LOSS', str(np.round(sum(all_glicko_loss)/len(all_glicko_loss),5)))

all_sea_data = pd.DataFrame(all_sea_data,columns=['Season','Elo','Glicko','L5'])
all_sea_data.to_csv('./data/yby_data.csv',index=False)
print(all_sea_data)

# end
