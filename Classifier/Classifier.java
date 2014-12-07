package org.myorg;

import java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.DistributedCache;

public class Classifier {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

  private File cachedFile;
  private BufferedReader cachedBufferedReader;
  private List<String> dictionary = new ArrayList<String>();
  private HashMap<String, HashMap<String, Integer>> dictionaryHashMap = new HashMap<String, HashMap<String, Integer>>();
  
  public void configure(JobConf job) {
    cachedFile = new File("./classWiseTitleBody.txt");
    try {
      createDictionaryHashMap(cachedFile);
    } catch (IOException ioe) {
      System.out.println(ioe.getMessage());
      System.exit(1);
    }
  }  

  private void createDictionaryHashMap(File f) throws IOException {
    cachedBufferedReader = new BufferedReader(new FileReader(f));
 
    String line = null;
    while ((line = cachedBufferedReader.readLine()) != null) {
      String[] keyValue = line.split("\\s+-->\\s+");
       if (keyValue.length>1) {	
	String[] titleBodyArray = keyValue[1].split(",\\s+");
	for (int i=0;i<titleBodyArray.length;i++) {
          String[] wordAndCount = titleBodyArray[i].split("@");
	    if (wordAndCount.length>1) {
	     if (dictionaryHashMap.get(keyValue[0]) == null) {
	      HashMap<String, Integer> tmpHashMap = new HashMap<String, Integer>();
	      tmpHashMap.put(wordAndCount[0], new Integer(Integer.parseInt(wordAndCount[1])));
	      dictionaryHashMap.put(keyValue[0], tmpHashMap);
	     } else {
		HashMap<String, Integer> tmpHashMap = dictionaryHashMap.get(keyValue[0]);
		tmpHashMap.put(wordAndCount[0], Integer.parseInt(wordAndCount[1]));
		dictionaryHashMap.put(keyValue[0], tmpHashMap);
	     }
	  }
	} 
       }
    }
  }

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();

    /**** Google's search Stopwords ****/
        private static Set<String> googleStopwords;

        static {
            googleStopwords = new HashSet<String>();
            googleStopwords.add("I"); googleStopwords.add("a"); googleStopwords.add("about");
            googleStopwords.add("an"); googleStopwords.add("are"); googleStopwords.add("as");
            googleStopwords.add("at"); googleStopwords.add("be"); googleStopwords.add("by");
            googleStopwords.add("com"); googleStopwords.add("de"); googleStopwords.add("en");
            googleStopwords.add("for"); googleStopwords.add("from"); googleStopwords.add("how");
            googleStopwords.add("in"); googleStopwords.add("is"); googleStopwords.add("it");
            googleStopwords.add("la"); googleStopwords.add("of"); googleStopwords.add("on");
            googleStopwords.add("or"); googleStopwords.add("that"); googleStopwords.add("the");
            googleStopwords.add("this"); googleStopwords.add("to"); googleStopwords.add("was");
            googleStopwords.add("what"); googleStopwords.add("when"); googleStopwords.add("where");
            googleStopwords.add("who"); googleStopwords.add("will"); googleStopwords.add("with");
            googleStopwords.add("and"); googleStopwords.add("the"); googleStopwords.add("www");
        }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      //String[] topicsString = line.split("</?TOPICS>");
      //String[] topicsArray = topicsString[1].split("</?D>");
      String sum = "";

      String[] textString = line.split("</?text[^>]*>");
      String[] titleSplit = line.split("</?title[^>]*>"); 

      String completeBodyString = "";
      String completeTitleString = "";
      if (textString.length>1) {
              //String[] titleString = textString[1].split("</?TITLE[^>]*>");
              //String[] bodyString = textString[1].split("</?BODY[^>]*>");
	      //String completeTitleString = "";
	      //String completeBodyString = "";

	      //if (titleString.length>1) {
		//completeTitleString = titleString[1];
	      //} 
	
	      //if (bodyString.length>1) {
		completeBodyString = textString[1];
	      //}
	}

	if (titleSplit.length>1) {
		completeTitleString = titleSplit[1];
	}
              /**************************Title****************************/
                StringTokenizer titleTokenizer = new StringTokenizer(completeTitleString);
                while (titleTokenizer.hasMoreTokens()) {
                  String nextToken = titleTokenizer.nextToken().toLowerCase();
		  if ( (googleStopwords.contains(nextToken)) || (!Character.isLetter(nextToken.charAt(0))) || (Character.isDigit(nextToken.charAt(0))) || (nextToken.contains("_")) || (nextToken.length() < 3)) { 
                        continue;
                  }
		  Set<String> outerMap = dictionaryHashMap.keySet();
		  Iterator<String> outerMapIterator = outerMap.iterator();
		  while (outerMapIterator.hasNext()) {
        	    String currentClass = outerMapIterator.next();
		    if (dictionaryHashMap.get(currentClass).containsKey(nextToken)) {
		      int termFrequency = dictionaryHashMap.get(currentClass).get(nextToken);
		      int numberOfClasses = dictionaryHashMap.size();
		      double probability = ((double)dictionaryHashMap.get(currentClass).get(nextToken))/((double)dictionaryHashMap.get(currentClass).get("TotalWords"));
		      double logProbability = Math.log(probability);
		      output.collect(new Text("<TITLE>"+completeTitleString+"</TITLE><BODY>"+completeBodyString+"</BODY><CLASS>"+currentClass+"</CLASS>"), new Text(Integer.toString(termFrequency)));
		      //output.collect(new Text("<TITLE>"+completeTitleString+"</TITLE><CLASS>"+currentClass+"</CLASS>"), new Text(Integer.toString(dictionaryHashMap.get(currentClass).get(nextToken))));
		    } else {
		      //output.collect(new Text("<TITLE>"+completeTitleString+"</TITLE><BODY>"+completeBodyString+"</BODY><CLASS>"+currentClass+"</CLASS>"), new IntWritable(0));
		      //output.collect(new Text("<TITLE>"+completeTitleString+"</TITLE><CLASS>"+currentClass+"</CLASS>"), new IntWritable(0));
		    }
		  }
                }

	      /*****************************Body***************************/
                StringTokenizer bodyTokenizer = new StringTokenizer(completeBodyString);
                while (bodyTokenizer.hasMoreTokens()) {
                  String nextToken = bodyTokenizer.nextToken().toLowerCase();
                  if ( (googleStopwords.contains(nextToken)) || (!Character.isLetter(nextToken.charAt(0))) || (Character.isDigit(nextToken.charAt(0))) || (nextToken.contains("_")) || (nextToken.length() < 3)) {
                        continue;
                  }
		  Set<String> outerMap = dictionaryHashMap.keySet();
                  Iterator<String> outerMapIterator = outerMap.iterator();
                  while (outerMapIterator.hasNext()) {
                    String currentClass = outerMapIterator.next();
                    if (dictionaryHashMap.get(currentClass).containsKey(nextToken)) {
		      int termFrequency = dictionaryHashMap.get(currentClass).get(nextToken);
		      double probability = ((double)dictionaryHashMap.get(currentClass).get(nextToken))/((double)dictionaryHashMap.get(currentClass).get("TotalWords"));
                      double logProbability = Math.log(probability); 
                      //output.collect(new Text("<TITLE>"+completeTitleString+"</TITLE><BODY>"+completeBodyString+"</BODY><CLASS>"+currentClass+"</CLASS>"), new IntWritable(dictionaryHashMap.get(currentClass).get(nextToken)));
		      output.collect(new Text("<TITLE>"+completeTitleString+"</TITLE><BODY>"+completeBodyString+"</BODY><CLASS>"+currentClass+"</CLASS>"), new Text(Integer.toString(termFrequency)));
		    } else {

		    } 
                }
              }

    
  }
 }

  public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += Integer.parseInt(values.next().toString());
      }
      String[] splitClass = key.toString().split("</?CLASS>");
      output.collect(new Text(splitClass[0]+" -->"), new Text(splitClass[1]+"@"+Integer.toString(sum)));
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      int maxBayesianValue = 0;
      String predictedClass = "";
      while (values.hasNext()) {
	String tmpString = values.next().toString();
	String[] splitTmpString = tmpString.split("@");
  	if (Integer.parseInt(splitTmpString[1]) > maxBayesianValue) {
		maxBayesianValue = Integer.parseInt(splitTmpString[1]);
		predictedClass = tmpString;
	}
        //sum += Integer.parseInt(values.next().toString());
        //output.collect(key, new Text(values.next().toString()));
      }
      output.collect(key, new Text("<HiveDelimiter>"+predictedClass));
    }
  }


  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Classifier.class);
    DistributedCache.addCacheFile(new URI("/user/mshaikh4/Project/Classifier/Dictionary/classWiseTitleBody.txt#classWiseTitleBody.txt"), conf);

    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Combine.class);
    conf.setReducerClass(Combine.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
