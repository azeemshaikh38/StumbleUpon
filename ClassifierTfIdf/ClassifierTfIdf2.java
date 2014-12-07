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

public class ClassifierTfIdf2 {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

  private File cachedFile;
  private BufferedReader cachedBufferedReader;
  private List<String> dictionary = new ArrayList<String>();
  private HashMap<String, HashMap<String, Integer>> dictionaryHashMap = new HashMap<String, HashMap<String, Integer>>();
  
  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String[] keyValueSplit = value.toString().split(" -->");
      if (keyValueSplit.length>1)
        output.collect(new Text(keyValueSplit[0]), new Text(keyValueSplit[1]));
  }
 }

  public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += Integer.parseInt(values.next().toString());
      }
      String[] splitClass = key.toString().split("</?CLASS>");
      output.collect(new Text(splitClass[0]), new Text(splitClass[1]+"@"+Integer.toString(sum)));
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      double maxBayesianValue = Double.NEGATIVE_INFINITY;
      String predictedClass = "";
      while (values.hasNext()) {
	String tmpString = values.next().toString();
	String[] splitTmpString = tmpString.replaceAll("\\s+", "").split("@");
  	if (Double.parseDouble(splitTmpString[1]) > maxBayesianValue) {
		maxBayesianValue = Double.parseDouble(splitTmpString[1]);
		predictedClass = splitTmpString[0];
	}
      }
      output.collect(key, new Text("<CLASS>"+predictedClass+"</CLASS>"));
    }
  }


  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(ClassifierTfIdf2.class);
    DistributedCache.addCacheFile(new URI("/user/mshaikh4/Project/Classifier/Dictionary/classWiseTitleBody.txt#classWiseTitleBody.txt"), conf);

    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Combine.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
