package com.assignment.hw5_Part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Hello world!
 *
 */
public class Driver 
{
    public static void main( String[] args ) throws Exception
    {
    	Configuration conf = new Configuration();
        
        if(args.length != 2){
            System.err.println("Usage: InvertedIndex <input> <output>");
            System.exit(2);
        }
        
        Path input = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        
        Job job = new Job(conf, "Inverted Index");
        
        job.setJarByClass(Part2_Mapper.class);
        job.setMapperClass(Part2_Mapper.class);
        job.setCombinerClass(Part2_Reducer.class);
        job.setReducerClass(Part2_Reducer.class);
        
        job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setPartitionerClass(MonthPartitioner.class);
        job.setNumReduceTasks(12);
        
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);
        
        FileSystem hdfs = FileSystem.get(conf);
        if(hdfs.exists(outputDir)) hdfs.delete(outputDir, true);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
