rm ./classWiseTitleBody.txt
cp ../ClassIdentifier/classWiseTitleBody.txt ./
hadoop fs -rm -r /user/mshaikh4/Project/Classifier/Dictionary/classWiseTitleBody.txt
hadoop fs -put ./classWiseTitleBody.txt /user/mshaikh4/Project/Classifier/Dictionary/classWiseTitleBody.txt

javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d Classes/ Classifier.java
jar -cvf Classifier.jar -C ./Classes .
hadoop fs -rm -r /user/mshaikh4/Project/Classifier/Output/
hadoop jar Classifier.jar org.myorg.Classifier /user/mshaikh4/Project/Classifier/Input/ /user/mshaikh4/Project/Classifier/Output/
rm -rf ./ClassifierPhase1Result
hadoop fs -get /user/mshaikh4/Project/Classifier/Output/part-00000 ./ClassifierPhase1Result


hadoop fs -rm -r /user/mshaikh4/Project/Classifier/Input2/*
hadoop fs -put ClassifierPhase1Result /user/mshaikh4/Project/Classifier/Input2/
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d Classes/ Classifier2.java
jar -cvf Classifier.jar -C ./Classes .
hadoop fs -rm -r /user/mshaikh4/Project/Classifier/Output2/
hadoop jar Classifier.jar org.myorg.Classifier2 /user/mshaikh4/Project/Classifier/Input2/ /user/mshaikh4/Project/Classifier/Output2/
rm -rf ./ClassifierPhase2Result
hadoop fs -get /user/mshaikh4/Project/Classifier/Output2/part-00000 ./ClassifierPhase2Result
