#!/usr/bin/python

'''
Written by hkarthik@umich.edu

'''

from pyspark import SparkContext
from pyspark.sql import SQLContext



if __name__ == "__main__":

    sc = SparkContext(appName="umsi618f22lab7")
    sqlc = SQLContext(sc)


    # load data using sqlc
    lol = sqlc.read.csv("/scratch/siads618f22_class_root/siads618f22_class/shared_data/lab7_data/euw_ranked_team.csv", header=True)
    lol.registerTempTable("lol")
    matchnumlol = sqlc.sql('''
    SELECT summonerId, COUNT(matchId) AS matchnum FROM lol
    GROUP BY summonerId
    ''')
    matchnumlol.createOrReplaceTempView("matchnumlol") # dflol contains DF values column, called df
    
    q1 = sqlc.sql('''
    SELECT lol.summonerId, lol.matchId, 
    (2*cast(lol.kill as int)+cast(lol.assist as int)-3*cast(lol.death as int)) AS df, matchnumlol.matchnum AS match_num FROM lol
    LEFT OUTER JOIN matchnumlol
    ON matchnumlol.summonerId=lol.summonerId
    WHERE matchnumlol.matchnum>=10
    ORDER BY df DESC, matchnumlol.matchnum DESC, summonerId ASC, matchId ASC
    ''')
    q1.createOrReplaceTempView("q1")
    q1.coalesce(1).write.option("delimiter","\t").csv("mingyuli_si618_lab7_output_s1.csv")
    # fill the csv function with the appropriate filename and parameters
    
    
    dflol = sqlc.sql('''
    SELECT *,(2*cast(kill as int)+cast(assist as int)-3*cast(death as int)) AS df FROM lol
    ''')
    dflol.createOrReplaceTempView("dflol")
    totaldf = sqlc.sql('''
    SELECT matchId, winner, ABS(SUM(df)) AS sumdf FROM dflol
    GROUP BY matchId, winner
    ORDER BY matchId, winner
    ''')
    totaldf.registerTempTable("totaldf") # the sum of df for all teams
    
    alldf = sqlc.sql('''
    SELECT dflol.summonerId, totaldf.matchId, totaldf.winner,
    CASE WHEN totaldf.sumdf=0 THEN 1 ELSE totaldf.sumdf END AS sumdf, 
    dflol.df, (dflol.df/totaldf.sumdf) AS normaldf, matchnumlol.matchnum FROM dflol
    LEFT OUTER JOIN totaldf
    ON dflol.matchId=totaldf.matchId AND dflol.winner=totaldf.winner
    LEFT OUTER JOIN matchnumlol
    ON dflol.summonerId=matchnumlol.summonerId
    ORDER BY totaldf.matchId, totaldf.winner
    ''')
    alldf.registerTempTable("alldf")
    
    avgnormal = sqlc.sql('''
    SELECT summonerId, CEILING(AVG(normaldf)) as avgnormaldf FROM alldf
    GROUP BY summonerId
    ORDER BY CEILING(AVG(normaldf)) DESC
    ''')
    avgnormal.registerTempTable("avgnormal")
    q2 = sqlc.sql('''
    SELECT DISTINCT alldf.summonerId, avgnormal.avgnormaldf, alldf.matchnum FROM alldf
    LEFT OUTER JOIN avgnormal
    ON alldf.summonerId=avgnormal.summonerId
    WHERE alldf.matchnum>=10
    ORDER BY avgnormal.avgnormaldf DESC, alldf.matchnum DESC
    ''')
    q2.coalesce(1).write.option("delimiter","\t").csv("mingyuli_si618_lab7_output_2.csv")
    
    
    diffdf = sqlc.sql('''
    SELECT DISTINCT p1.matchId, p1.predictedRole, p1.summonerId AS p1id, p2.summonerId AS p2id,
    ABS(p1.df-p2.df) AS dfdiff FROM dflol p1, dflol p2
    WHERE p1.matchId=p2.matchId AND p1.predictedRole=p2.predictedRole AND p1.summonerId>p2.summonerId AND p1.winner!=p2.winner
    ORDER BY dfdiff DESC
    ''')
    diffdf.registerTempTable("diffdf")
    selectmax = sqlc.sql('''
    SELECT matchId, MAX(dfdiff) AS maxdiff FROM diffdf
    GROUP BY matchId
    ORDER BY MAX(dfdiff) DESC
    ''')
    selectmax.registerTempTable("selectmax")
    q3 = sqlc.sql('''
    SELECT diffdf.matchId, diffdf.predictedRole, selectmax.maxdiff, diffdf.p1id, diffdf.p2id FROM selectmax
    LEFT OUTER JOIN diffdf
    ON selectmax.matchId=diffdf.matchId
    WHERE diffdf.dfdiff=selectmax.maxdiff
    ORDER BY selectmax.maxdiff DESC, diffdf.predictedRole, diffdf.matchId, diffdf.p1id, diffdf.p2id
    ''')
    q3.coalesce(1).write.option("delimiter","\t").csv("mingyuli_si618_lab7_output_3.csv")