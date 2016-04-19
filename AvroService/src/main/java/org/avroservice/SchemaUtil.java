package org.avroservice;

import org.apache.avro.Schema;

public class SchemaUtil {
	
	public static Schema parseAvscToAvroSchema(final String avsc)
	{
		final Schema avroSchema=new Schema.Parser().parse(avsc);
		
		return avroSchema;
	}

}
