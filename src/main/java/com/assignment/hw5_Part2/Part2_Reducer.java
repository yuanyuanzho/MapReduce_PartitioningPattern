package com.assignment.hw5_Part2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Part2_Reducer extends Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterable<Text> values,Context context) 
			throws IOException, InterruptedException{
		for(Text t : values){
			context.write(key,t);
		}
		
	}
}
