package com.assignment.hw5_Part2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Part2_Mapper extends Mapper<Object, Text, Text, Text> {
	private final static SimpleDateFormat date = new SimpleDateFormat("dd/MMM/yyyy");
	private final static SimpleDateFormat month = new SimpleDateFormat("MMM");
	
	private Text outkey = new Text();
			
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		try {
			
			String[] fields = value.toString().split("- -");
			Pattern regex = Pattern.compile("(\\[[^\\]]*\\])"); //方括号
			Matcher match = regex.matcher(fields[1]); 
			
			if(match.find()){
				String str = match.group().substring(1, match.group().length()-1);
				
				Date d1 = date.parse(str);
				String monthStr = month.format(d1);
				outkey.set(monthStr);
				context.write(outkey, value);
				
			}
			
			
		} catch (ParseException e) {
			e.printStackTrace();
		}catch (NullPointerException ex){
			;
		} 
		
	}
	
}


















