package org.avroservice.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.avroservice.mapper.AvroDataConverterMapper;
import org.avroservice.mapper.AvroDataConverterReducer;

public class AvroDataConverterJob extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		Configuration conf = super.getConf();
		Job avroDataConverterJob = Job.getInstance(conf, this.getClass().getName());
		avroDataConverterJob.setJarByClass(AvroDataConverterJob.class);

		FileInputFormat.addInputPath(avroDataConverterJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(avroDataConverterJob, new Path(args[1]));
		
		avroDataConverterJob.setMapperClass(AvroDataConverterMapper.class);
		avroDataConverterJob.setReducerClass(AvroDataConverterReducer.class);
		
		avroDataConverterJob.submit();
		return 0;
	}
}
