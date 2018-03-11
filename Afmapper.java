package org;

import java.io.IOException;
//
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Afmapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	private Text outkey=new Text();
	private IntWritable outvalue=new IntWritable(1);
 @Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		String record = value.toString().trim();
		String[] fields = record.split(" ");
		outkey.set(fields[0].toUpperCase());
		context.write(outkey, outvalue);
	}}

	
