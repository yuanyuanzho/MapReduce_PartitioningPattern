package com.assignment.hw5_Part2;

import org.apache.commons.net.ftp.Configurable;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.sun.tools.doclets.internal.toolkit.Configuration;

public class MonthPartitioner extends 
	Partitioner<Text, Text> {

	

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		String months=key.toString();
		
		if (months.equals("Jan")) {
		    return 0;	
		}else if (months.equals("Feb")) {
			return 1;
		}else if (months.equals("Mar")) {
			return 2;
		}else if (months.equals("Apr")) {
			return 3;
		}else if (months.equals("May")) {
			return 4;
		}else if (months.equals("Jun")) {
			return 5;
		}else if (months.equals("Jul")) {
			return 6;
		}else if (months.equals("Aug")) {
			return 7;
		}else if (months.equals("Sep")) {
			return 8;
		}else if (months.equals("Oct")) {
			return 9;
		}else if (months.equals("Nov")) {
			return 10;
		}else if(months.equals("Dec")){
			return 11;
		}
//		else  {
//		return 1;
		return 1;
		}
	}

	



