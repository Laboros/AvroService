package org.avroservice;

import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang.StringUtils;
import org.laboros.StorageFormat;

public class AvroStorageFormat implements StorageFormat {
	private Schema oSchema;
	private boolean isFixedWidth=Boolean.FALSE;
	private String delimiter=AvroConstants.SEPARATOR_COMMA;// Default separator
	
	public AvroStorageFormat(){
		
	}
	
	public void setAVSC(final String avsc){
		oSchema=SchemaUtil.parseAvscToAvroSchema(avsc);
	}
	/**
	 * @param avsc
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
		
		GenericRecord record=null;
		
		if(isFixedWidth()){
			record=parseFixedWidthData(inputData);
		}else{
			record=parseDelimitedData(inputData);
		}
		
		return record;
	}

	public GenericRecord parseFixedWidthData(final String inputData)
	{
		GenericRecord record=null;
		if(oSchema!=null){
			GenericRecordBuilder builder=new GenericRecordBuilder(oSchema);
			record=builder.build();
			List<Field> fields=oSchema.getFields();
			
		}
		
		return record;
	}
	
	public GenericRecord parseDelimitedData(final String inputData)
	{
		GenericRecord record=null;
		if(getDelimiter()!=null && oSchema!=null)
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
	
			}else{
				throw new AvroRuntimeException("No schema exists");
			}
		return record;
	}
	

}
