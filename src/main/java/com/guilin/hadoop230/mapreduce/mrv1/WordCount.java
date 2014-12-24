package com.guilin.hadoop230.mapreduce.mrv1;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class WordCountMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				output.collect(word, one);
			}

		}
	}

	public static class WordCountReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			result.set(sum);
			output.collect(key, result);
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		 String input = "hdfs://master:9000/data/wordcount";
		 String output = "hdfs://master:9000/data/output/wordcount";

		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("WordCount");

//		String[] otherArgs = new GenericOptionsParser(conf, args)
//				.getRemainingArgs();
//		if (otherArgs.length != 2) {
//			System.err.println("Usage: wordcount <in> <out>");
//			System.exit(2);
//		}
		
		conf.set("dfs.namenode.secondary.http-address", "master:9001");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("fs.defaultFS", "hdfs://master:9000");
		conf.set("yarn.resourcemanager.address", "master:8032");

//		 conf.addResource("classpath:/hadoop2/core-site.xml");
//		 conf.addResource("resources/hadoop2/core-site.xml");
//		 conf.addResource("resources/hadoop2/hdfs-site.xml");
//		 conf.addResource("resources/hadoop2/mapred-site.xml");
//		 conf.addResource("resources/hadoop2/yarn-site.xml");
		 conf.set("mapred.jar", "myhadoop.jar");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(WordCountMapper.class);
		conf.setCombinerClass(WordCountReducer.class);
		conf.setReducerClass(WordCountReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

//		FileInputFormat.setInputPaths(conf, new Path(otherArgs[0]));
//		FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);
	}

}