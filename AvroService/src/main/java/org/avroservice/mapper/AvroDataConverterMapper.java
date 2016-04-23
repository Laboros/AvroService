package org.avroservice.mapper;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.avroservice.AvroConstants;

public class AvroDataConverterMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	boolean isFixedWidth=Boolean.FALSE;
	String delimiter=null;
	Schema oSchema=null;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf=context.getConfiguration();
		
		delimiter=conf.get(AvroConstants.DELIMITER.getValue());
		if(StringUtils.isEmpty(delimiter))
		{
			isFixedWidth=Boolean.TRUE;
		}
		final String avsc=conf.get(AvroConstants.AVSC.getValue());
		if(!StringUtils.isEmpty(avsc))
		{
			oSchema=new Schema.Parser().parse(avsc);
		}
		
	};

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException 
			{
		
	};
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
	};

}
