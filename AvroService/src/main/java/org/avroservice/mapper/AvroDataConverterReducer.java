package org.avroservice.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvroDataConverterReducer extends
		Reducer<Text, Text, Text, Text> {


	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
	};

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
	};

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
	};

}
