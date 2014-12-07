package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ClassIdentifier {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
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
      String[] topicsString = line.split("</?TOPICS>");
      String[] topicsArray = topicsString[1].split("</?D>");
      String sum = "";
      
      String[] textString = line.split("</?TEXT[^>]*>");
     
      if (textString.length>1) {
	      String[] titleString = textString[1].split("</?TITLE[^>]*>");
	      String[] bodyString = textString[1].split("</?BODY[^>]*>");

	      /**************************Title****************************/	
	      if (titleString.length>1) {
		StringTokenizer titleTokenizer = new StringTokenizer(titleString[1]);
	      	while (titleTokenizer.hasMoreTokens()) {
		  String nextToken = titleTokenizer.nextToken().toLowerCase();
		  if ( (googleStopwords.contains(nextToken)) || (!Character.isLetter(nextToken.charAt(0))) || (Character.isDigit(nextToken.charAt(0))) || (nextToken.contains("_")) || (nextToken.length() < 3)) {
			continue;
		  }
		  for (int i=1; i<topicsArray.length; i++) {
		    if ((!topicsArray[i].isEmpty()) && (!nextToken.isEmpty()))
	              output.collect(new Text(topicsArray[i]+" -->"), new Text(nextToken+"@1"));
		  }
	 	}
	      }

	      /*****************************Body***************************/
	      if (bodyString.length>1) {
                StringTokenizer bodyTokenizer = new StringTokenizer(bodyString[1]);
                while (bodyTokenizer.hasMoreTokens()) {
                  String nextToken = bodyTokenizer.nextToken().toLowerCase();
                  if ( (googleStopwords.contains(nextToken)) || (!Character.isLetter(nextToken.charAt(0))) || (Character.isDigit(nextToken.charAt(0))) || (nextToken.contains("_")) || (nextToken.length() < 3)) {
                        continue;
                  }
                  for (int i=1; i<topicsArray.length; i++) {
                    if ((!topicsArray[i].isEmpty()) && (!nextToken.isEmpty()))
                      output.collect(new Text(topicsArray[i]+" -->"), new Text(nextToken+"@1"));
                  }
                }
              }

      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String sum = "";
      Hashtable<String, Integer> wordcount = new Hashtable<String, Integer>();
      int totalWords = 0;

      while (values.hasNext()) {
	String[] tmp = values.next().toString().split("@");
    	if (tmp.length>1) {
	    if (wordcount.containsKey(tmp[0])) {
		int newCount = wordcount.get(tmp[0]) + Integer.parseInt(tmp[1]);
		wordcount.put(tmp[0], newCount);
	    } else {
	    	wordcount.put(tmp[0], Integer.parseInt(tmp[1]));
	    }
	}
	//output.collect(key, new Text(values.next().toString()));
        //sum += values.next().toString()+", ";
      }

      Enumeration<String> wordcountKeys = wordcount.keys();
      while(wordcountKeys.hasMoreElements()) {
	String tmpKey = wordcountKeys.nextElement();
	totalWords += wordcount.get(tmpKey);
	sum += tmpKey+"@"+Integer.toString(wordcount.get(tmpKey))+", ";
      }
      output.collect(key, new Text("TotalWords@"+Integer.toString(totalWords)+", "+sum));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(ClassIdentifier.class);
    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
