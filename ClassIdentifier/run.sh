hadoop fs -rm -r /user/mshaikh4/Project/ClassIdentifier/Output/
hadoop jar org.myorg.ClassIdentifier /user/mshaikh4/Project/ClassIdentifier/Input/ /user/mshaikh4/Project/ClassIdentifier/Output/
rm -rf ./classWiseTitleBody.txt
hadoop fs -get /user/mshaikh4/Project/ClassIdentifier/Output/part-00000 ./classWiseTitleBody.txt 
