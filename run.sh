hadoop fs -rm -r /user/mshaikh4/Project/Classifier/Input/*

for i in $@
do
	hadoop fs -put $i /user/mshaikh4/Project/Classifier/Input/
done

cd ClassIdentifier/
./run.sh
cd ../
cd Classifier/
./run.sh
cd ../
cd GetPageStat/
./run.sh
cd ../
cd PageStatHive/
./run.sh
cd ../
cd UserQueryHive/
./run.sh
