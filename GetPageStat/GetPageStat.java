package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class GetPageStat {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] pageStatString = line.split("\\s*</?PAGESTAT>");
      if (pageStatString.length > 1) {
        output.collect(new Text(pageStatString[0]), new Text(pageStatString[1]));
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      float sum = 0;
      while (values.hasNext()) {
        sum += Float.parseFloat(values.next().toString());
      }
      output.collect(key, new Text("<PAGESTAT>"+Float.toString(sum)+"</PAGESTAT>"));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(GetPageStat.class);
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
