javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d Classes/ GetPageStat.java
jar -cvf GetPageStat.jar -C Classes/ .
hadoop fs -rm /user/mshaikh4/Project/GetPageStat/Input/PageStat.txt
hadoop fs -put ./PageStat.txt /user/mshaikh4/Project/GetPageStat/Input/PageStat.txt
hadoop fs -rm -r /user/mshaikh4/Project/GetPageStat/Output/
hadoop jar GetPageStat.jar org.myorg.GetPageStat /user/mshaikh4/Project/GetPageStat/Input/ /user/mshaikh4/Project/GetPageStat/Output/
rm -rf ./PageStat.txt
hadoop fs -get /user/mshaikh4/Project/GetPageStat/Output/part-00000 ./PageStat.txt
