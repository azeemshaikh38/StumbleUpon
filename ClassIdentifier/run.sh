javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d Classes/ ClassIdentifier.java
jar -cvf ClassIdentifier.jar -C ./Classes .
hadoop fs -rm -r /user/mshaikh4/Project/ClassIdentifier/Output/
hadoop jar ClassIdentifier.jar org.myorg.ClassIdentifier /user/mshaikh4/Project/ClassIdentifier/Input/ /user/mshaikh4/Project/ClassIdentifier/Output/
rm -rf ./classWiseTitleBody.txt
hadoop fs -get /user/mshaikh4/Project/ClassIdentifier/Output/part-00000 ./classWiseTitleBody.txt 
perl getTfIdf.pl
#hadoop fs -rm -r /user/mshaikh4/Project/Classifier/Input/*
#hadoop fs -put ./classWiseTitleBody.txt /user/mshaikh4/Project/Classifier/Input/classWiseTitleBody.txt
