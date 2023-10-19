#!/usr/bin/env python
# coding: utf-8

# In[8]:


def cleaning_dataframe(column_list,data_file):
    cleaned_df = data_file.dropna(subset=column_list)
    return cleaned_df
    
def drop_Duplicates(data_file):
    non_duplicates_df = data_file.dropDuplicates(subset=["Year","Player"])   
    return non_duplicates_df

def career_points_top():
    nba_stats_df = cleaning_dataframe(['Player','PTS'],nba_season_data)
    nba_stats_df = drop_Duplicates(nba_stats_df)
    nba_top_scorer_df = nba_stats_df.groupby('Player').agg(F.sum('PTS').alias('TOTAL_POINTS')).sort('TOTAL_POINTS',ascending=False)
    return nba_top_scorer_df


def three_pointer_stats():
    three_pointer_df = cleaning_dataframe(['3P','Year'],nba_season_data)
    three_pointer_df = drop_Duplicates(three_pointer_df)
    three_pointer_df = three_pointer_df.groupby('Year').agg(F.sum('3P').alias('Total_3P'),
                                                  round(F.mean('3PA'),2).alias('Total_3PA'),
                                                    round(F.mean('3P%')*100,2).alias('Total_3P%')).sort('Year')
    
    return(three_pointer_df)


def three_pointer_players(from_year,to_year):
    
    three_pointer_players_df = cleaning_dataframe(['Player','3P%','3P','3PA','Year'],nba_season_data)
    three_pointer_players_df = drop_Duplicates(three_pointer_players_df)
    three_pointer_players_df = three_pointer_players_df.filter(col('Year').between(from_year,to_year))
    three_pointer_players_df = three_pointer_players_df.groupby('Player').agg(F.sum('3P').alias('Total_3P')).sort('Total_3P',ascending=False)
    return three_pointer_players_df


def shots_breakdown():
    yearly_shots_brkdwn_df = cleaning_dataframe(['Year','2P','3P','FT','Player'],nba_season_data)
    yearly_shots_brkdwn_df = drop_Duplicates(yearly_shots_brkdwn_df)
    yearly_shots_brkdwn_df = yearly_shots_brkdwn_df.groupby('Year').agg(round(F.sum('2P'),2).alias('AVG_2P'),
                                                                       round(F.sum('3P'),2).alias('AVG_3P'),
                                                                       round(F.sum('FT'),2).alias('AVG_FT')).sort('Year')
    total = sum(yearly_shots_brkdwn_df[c] for c in yearly_shots_brkdwn_df.columns[1:])
    yearly_shots_brkdwn_df = yearly_shots_brkdwn_df.select(yearly_shots_brkdwn_df.columns[0], 
                                                           *[round((yearly_shots_brkdwn_df[c] / total)*100,2).alias(c) for c in yearly_shots_brkdwn_df.columns[1:]])
    return yearly_shots_brkdwn_df

def nba_collegewise_stats():
    nba_player_data_df = cleaning_dataframe(['Player','college'],nba_player_state_data)
    nba_player_data_df = nba_player_data_df.drop('_c0')
    nba_player_college_df = nba_player_data_df.groupby('college').agg(F.count('Player').alias('Number_of_Players'))
    nba_player_college_df = nba_player_college_df.sort('Number_of_Players',ascending=False)
    return nba_player_college_df

def nba_statewise_stats():
    
    nba_player_data_df = cleaning_dataframe(['Player','birth_state'],nba_player_state_data)
    nba_player_data_df = nba_player_data_df.drop('_c0')
    nba_player_state_df = nba_player_data_df.groupby('birth_state').agg(F.count('Player').alias('Number_of_Players'))
    nba_player_state_df = nba_player_state_df.select(col('birth_state').alias('State'), 
                                                     col('Number_of_Players').alias('Number_of_Players'))
    nba_player_state_df = nba_player_state_df.sort('Number_of_Players',ascending=False)
    total_players = nba_player_state_df.groupby().sum('Number_of_Players').collect()
    total_players = int(total_players[0][0])
    nba_player_state_df = nba_player_state_df.withColumn('Percent_of_players',
                                                        round((nba_player_state_df.Number_of_Players/total_players*100),2))


    us_census_df = us_census_data.groupby('State').mean()
    n = len(us_census_df.columns) - 1
    us_census_df = us_census_df.withColumn('Mean',
                                           sum(us_census_df[col] for col in us_census_df.columns[1:])/n)

    total_population = us_census_df.agg(F.max('Mean')).collect()

    us_census_df = us_census_df.withColumn('Percent_of_Population',
                                          round((us_census_df.Mean/total_population[0][0]*100),2))
    us_census_df = us_census_df[['State','Percent_of_Population']]

    nba_state_ratio = nba_player_state_df.join(us_census_df,'State','outer')
    nba_state_ratio = nba_state_ratio.sort('Percent_of_players',ascending=False)

    return nba_state_ratio

def nba_collegwise_player_position_stats():
    nba_player_position_df = cleaning_dataframe(['name','college','position'],nba_player_position_data)
    nba_player_position_df = nba_player_position_df.groupby('college','position').agg(F.count('name').alias('Total_Players'))
    nba_player_position_df = nba_player_position_df.sort('Total_Players',ascending=False)
    return nba_player_position_df


def nba_champions_stats():
    nba_champions_df = cleaning_dataframe(['year','Champion','Opponent'],nba_champions_data)
    lookupDict = {'Minneapolis Lakers':'Los Angeles Lakers', 'Syracuse Nationals':'Philadelphia 76ers',
                      'Philadelphia Warriors':'Philadelphia 76ers', 'Fort Wayne Pistons':'Detroit Pistons',
                     'St. Louis Hawks':'Atlanta Hawks','San Francisco Warriors':'Golden State Warriors',
                     'Baltimore Bullets':'Washington Wizards','Seattle SuperSonics':'Oklahoma City Thunder',
                     'New Jersey Nets':'Brooklyn Nets','Washington Bullets':'Washington Wizards'}

    nba_champions_df = nba_champions_df.na.replace(lookupDict,1,'Champion')
    nba_champions_df = nba_champions_df.na.replace(lookupDict,1,'Opponent')


    nba_winners = nba_champions_df.groupby('Champion').agg(F.count('Champion').alias('Titles'))
    nba_winners = nba_winners.select(col('Champion').alias('Team'), 
                                                         col('Titles').alias('Titles'))

    nba_runners = nba_champions_df.groupby('Opponent').agg(F.count('Opponent').alias('Runners_Up'))
    nba_runners = nba_runners.select(col('Opponent').alias('Team'), 
                                                         col('Runners_Up').alias('Runners_Up'))

    nba_champions_history = nba_winners.join(nba_runners,'State','outer')
    nba_champions_history = nba_champions_history.na.fill(0).sort('Titles','Runners_Up',ascending=False)
    return nba_champions_history

def files_from_args(): 
    import argparse 
    parser = argparse.ArgumentParser() 
    parser.add_argument('-i', '--input', default='input')  
    args = parser.parse_args() 
    return (args.input)
    

if __name__ == "__main__":
    start_time = time()
    import findspark
    findspark.init()
    
    from pyspark.sql import SparkSession
    from pyspark.sql import SQLContext
    from pyspark import SparkContext
    import pyspark.sql.functions as F
    from pyspark.sql.functions import col
    from pyspark.sql.functions import round
    from pyspark.sql import DataFrame
    from time import time
    from pyspark.sql.types import *

    spark = SparkSession.builder.master('local').appName('Test').getOrCreate()
    
    #path = files_from_args()

    reader = spark.read
    path = 'C:\\Hardik\\Studies\\Sem II\\Big Data\\Project\\nba-players-stats\\'
    reader.option('header','True')
    reader.option('inferSchema','True')
    nba_champions_data = reader.format('csv').csv(path + 'nba-champions.csv')
    nba_player_position_data = reader.format('csv').csv(path + 'player-data.csv')
    nba_player_state_data = reader.format('csv').csv(path +'Players.csv')
    nba_season_data = reader.format('csv').csv(path+'Seasons-Stats.csv')
    us_census_data = reader.format('csv').csv(path+'US-statewise-census.csv')
    
    t0 = time()
    
    all_time_high_df = career_points_top()
    all_time_high_df.show(10,False)
    
    t1 = time()
    
    all_time_three_pointer_stats_df = three_pointer_stats()
    three_pointer_players_responsible_df = three_pointer_players(2001,2017)
    all_time_three_pointer_stats_df.show(10,False)
    three_pointer_players_responsible_df.show(10,False)
    
    t2 = time()
    
    all_time_points_breakdown_df = shots_breakdown()
    all_time_points_breakdown_df.show(10,False)
    
    t3 = time()
    
    collegewise_stats_df = nba_collegewise_stats()
    statewise_stats_df = nba_statewise_stats()
    player_position_collegewise_df = nba_collegwise_player_position_stats()
    collegewise_stats_df.show(10,False)
    statewise_stats_df.show(10,False)
    player_position_collegewise_df.show(10,False)
    
    t4 = time()
    
    nba_championship_stats_df = nba_champions_stats()
    nba_championship_stats_df.show(10,False)
    
    t5 = time()
    
    time_stats = [["Project Sections","Time in milliseconds"],["Part 1",(t1-t0)*1000],["Part 2",(t2-t1)*1000],["Part 3",(t3-t2)*1000],
                  ["Part 4",(t4-t3)*1000],["Part 5",(t5-t4)*1000],["Total",(t5-start_time)*1000]]
    
    write_file = path + 'time_stats.csv'
    with open(write_file,'w') as outfile:
        for line in time_stats:
            outfile.write(','.join(line))
            
    time_df = reader.format('csv').csv(path + write_file)
    time_df.show()
    spark.stop()


# In[ ]:




