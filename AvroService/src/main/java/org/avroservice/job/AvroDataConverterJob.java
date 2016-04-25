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
import org.avroservice.AvroConstants;
import org.avroservice.mapper.AvroDataConverterMapper;
import org.hdfsservice.util.HDFSUtil;

/**
 * 
 * @author svaduka
 *
 *Creates Avro files at specified location on hdfs
 */
public class AvroDataConverterJob extends Configured implements Tool {

	/**
	 * args[0] -- hdfspathtoavsc
	 * args[1] -- hdfspathtodata
	 * args[2] -- datadelimiter
	 * args[3] -- hdfspath to output -- this will be used by hive
	 */
	public int run(String[] args) throws Exception {
		
		Configuration conf = super.getConf();
		conf.set("mapreduce.framework.name", "local");
		
		final String hdfsPathToAVSC=args[0];
		
		final String avsc=HDFSUtil.readDataFromHDFS(hdfsPathToAVSC, conf);
		
		conf.set(AvroConstants.AVSC.getValue(), avsc);
		conf.set(AvroConstants.DELIMITER.getValue(), args[2]);
		
		Job avroDataConverterJob = Job.getInstance(conf, this.getClass().getName());
		
		avroDataConverterJob.setJarByClass(AvroDataConverterJob.class);

		avroDataConverterJob.setInputFormatClass(TextInputFormat.class);
		
		TextInputFormat.addInputPath(avroDataConverterJob, new Path(args[1]));
		
		avroDataConverterJob.setMapOutputKeyClass(GenericRecord.class);
		avroDataConverterJob.setMapOutputValueClass(NullWritable.class);
		
		avroDataConverterJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		
		avroDataConverterJob.setOutputKeyClass(GenericRecord.class);
		avroDataConverterJob.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(avroDataConverterJob, new Path(args[3]));
		
		avroDataConverterJob.setMapperClass(AvroDataConverterMapper.class);
//		avroDataConverterJob.setReducerClass(AvroDataConverterReducer.class);
		
		avroDataConverterJob.submit();
		
		return 0;
	}
}
