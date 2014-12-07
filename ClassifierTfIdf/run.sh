rm ./classWiseTfIdf.txt
cp ../ClassIdentifier/classWiseTfIdf.txt ./
hadoop fs -rm -r /user/mshaikh4/Project/Classifier/Input/*
hadoop fs -put ./classWiseTfIdf.txt /user/mshaikh4/Project/Classifier/Input/classWiseTitleBody.txt

javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d Classes/ ClassifierTfIdf.java
jar -cvf ClassifierTfIdf.jar -C ./Classes .
hadoop fs -rm -r /user/mshaikh4/Project/Classifier/Output/
hadoop jar ClassifierTfIdf.jar org.myorg.ClassifierTfIdf /user/mshaikh4/Project/Classifier/Input/ /user/mshaikh4/Project/Classifier/Output/
rm -rf ./ClassifierTfIdfPhase1Result
hadoop fs -get /user/mshaikh4/Project/Classifier/Output/part-00000 ./ClassifierTfIdfPhase1Result


hadoop fs -rm -r /user/mshaikh4/Project/Classifier/Input2/*
hadoop fs -put ClassifierTfIdfPhase1Result /user/mshaikh4/Project/Classifier/Input2/
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d Classes/ ClassifierTfIdf2.java
jar -cvf ClassifierTfIdf.jar -C ./Classes .
hadoop fs -rm -r /user/mshaikh4/Project/Classifier/Output2/
hadoop jar ClassifierTfIdf.jar org.myorg.ClassifierTfIdf2 /user/mshaikh4/Project/Classifier/Input2/ /user/mshaikh4/Project/Classifier/Output2/
rm -rf ./ClassifierTfIdfPhase2Result
hadoop fs -get /user/mshaikh4/Project/Classifier/Output2/part-00000 ./ClassifierTfIdfPhase2Result
