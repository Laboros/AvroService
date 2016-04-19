package org.avroservice;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;
import org.laboros.StorageFormat;

public class AvroStorageFormat implements StorageFormat {
	private Schema oSchema;
	private boolean isFixedWidth=Boolean.FALSE;
	private String delimiter=AvroConstants.SEPARATOR_COMMA;// Default seperator
	
	/**
	 * 
	 * @param avsc
	 * 
	 * This method will create a avro Schema Object
	 */
	public AvroStorageFormat(final String avsc){
		oSchema=SchemaUtil.parseAvscToAvroSchema(avsc);
	}

	
	public String getDelimiter() {
		return delimiter;
	}

	/**
	 * @param delimiter
	 * Please set the delimiter for the input data if exist
	 */
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}


	public boolean isFixedWidth() {
		return isFixedWidth;
	}

	/**
	 * @param isFixedWidth
	 * This method will be used to set if we need to parse the fixed width file
	 */
	public void setFixedWidth(boolean isFixedWidth) {
		this.isFixedWidth = isFixedWidth;
	}

	public Object parseData(String inputData) {
		
		return null;
	}

	public GenericRecord parseFixedWidthData(final String inputData)
	{
		return null;
	}
	
	public GenericRecord parseDelimitedData(final String inputData)
	{
		GenericRecord record=null;
		if(getDelimiter()!=null)
		{

			final String delimiter=getDelimiter();
			
			GenericRecordBuilder builder=new GenericRecordBuilder(oSchema);
			record=builder.build();;
			List<Field> fields=oSchema.getFields();

			String[] columns=StringUtils.splitPreserveAllTokens(delimiter);
			
			for (int i = 0; i < fields.size(); i++) 
			{
				if(i<=columns.length)
				{
				record.put(i, columns[i]);
				}
			}
	
			}
		return record;
	}
	

}
