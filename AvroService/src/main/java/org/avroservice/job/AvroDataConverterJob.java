package org.avroservice.job;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.avroservice.mapper.AvroDataConverterMapper;
import org.avroservice.mapper.AvroDataConverterReducer;

public class AvroDataConverterJob extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		Configuration conf = super.getConf();
		
		Job avroDataConverterJob = Job.getInstance(conf, this.getClass().getName());
		
		avroDataConverterJob.setJarByClass(AvroDataConverterJob.class);

		avroDataConverterJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(avroDataConverterJob, new Path(args[0]));
		
		avroDataConverterJob.setMapOutputKeyClass(GenericRecord.class);
		avroDataConverterJob.setMapOutputValueClass(NullWritable.class);
		
		avroDataConverterJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		
		avroDataConverterJob.setOutputKeyClass(GenericRecord.class);
		avroDataConverterJob.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(avroDataConverterJob, new Path(args[1]));
		
		avroDataConverterJob.setMapperClass(AvroDataConverterMapper.class);
		avroDataConverterJob.setReducerClass(AvroDataConverterReducer.class);
		
		avroDataConverterJob.submit();
		
		return 0;
	}
}
