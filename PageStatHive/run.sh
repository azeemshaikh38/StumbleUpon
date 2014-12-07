#Create and Load Articles Table
rm ./exampleInput.txt
#cp ../ClassifierTfIdf/ClassifierTfIdfPhase2Result ./exampleInput.txt
cp ../Classifier/ClassifierPhase2Result ./exampleInput.txt

hadoop fs -rm /user/mshaikh4/Project/PageStat/Input/exampleInput.txt
hadoop fs -put ./exampleInput.txt /user/mshaikh4/Project/PageStat/Input/
hive -e 'use mshaikh4; drop table articles; create table articles (title string, body string, class string) row format serde "org.apache.hadoop.hive.serde2.RegexSerDe" WITH SERDEPROPERTIES ( "input.regex" = "^\\s*<TITLE>(.*)</TITLE>.*<BODY>(.*)</BODY>\\s+<CLASS>(.*)</CLASS>$", "output.format.string" = "%1$s %2$s %3$s") STORED AS TEXTFILE;'
hive -e 'use mshaikh4; load data inpath "/user/mshaikh4/Project/PageStat/Input/exampleInput.txt" overwrite into table articles;'
hive -e 'use mshaikh4; select * from articles;' > LoadArticlesResult;

#Create and Load PageStatistic Table
rm ./PageStat.txt
cp ../GetPageStat/PageStat.txt ./
hadoop fs -rm /user/mshaikh4/Project/PageStat/Input/PageStat.txt 
hadoop fs -put ./PageStat.txt /user/mshaikh4/Project/PageStat/Input/
hive -e 'use mshaikh4; drop table pagestats; create table pagestats (title string, pagestat double) row format serde "org.apache.hadoop.hive.serde2.RegexSerDe" WITH SERDEPROPERTIES ("input.regex" = "^<TITLE>(.*)</TITLE>.*<PAGESTAT>(.*)</PAGESTAT>$", "output.format.string" = "%1$s %2$s") STORED AS TEXTFILE;'
hive -e 'use mshaikh4; load data inpath "/user/mshaikh4/Project/PageStat/Input/PageStat.txt" overwrite into table pagestats;'
hive -e 'use mshaikh4; select * from pagestats;' > LoadPageStatsResult;


#Join above two tables for complete analyzed web table
hive -e 'use mshaikh4; drop table finalwebtable; create table finalwebtable as select a.title, a.body, a.class, b.pagestat from articles a join (select * from pagestats sort by pagestat ASC) b on (a.title=b.title);' 
hive -e 'use mshaikh4; select * from finalwebtable;' > LoadFinalWebTableResult;


#hive -e 'use mshaikh4; drop table batting; create table batting (playerid string, year int, stint int, team string, league string, games int, games_batted int, atbat int, runs int, hits int, secondbase int, thirdbase int, homerun int, rbi int, sb int, cs int, bb int, so int, ibb int, hbp int, sh int, sf int, gidp int, gold int) row format delimited fields terminated by ",";'
#hive -e 'use mshaikh4; load data inpath "/user/mshaikh4/Project/PageStat/Input/exampleInput.txt" overwrite into table batting;'
#hive -e 'use mshaikh4; drop table bestBatters; create table bestBatters as select a.year, a.playerid, a.runs from batting a join (select year, max(runs) runs from batting group by year) b on (a.year = b.year and a.runs = b.runs);'
#hive -e 'use mshaikh4; select * from articles;' > LoadArticlesResult;
