package org.avroservice;

public enum AvroConstants {
	
	SEPARATOR_COMMA(","),
	AVSC("AVSC"), 
	DELIMITER("DELIMITER"), 
	FILE_SEPARATOR(System.getProperty("file.separator")), 
	ZERO(0), SEPERATOR_PIPE("|"), 
	EMPTY(""), 
	FALSE("N"),
//AVSC CONVERSION
	TYPE("type"),
	TYPE_RECORD("record"),
	TABLE_NAME("name"),
	SOURCE_NAME("source"),
	NAMESPACE("namespace"),
	DOC("doc"),
	FIELDS("fields"),
	FIELD_NAME("name"),
	FIELD_DOC("doc"),
	FIELD_TYPE("type"),
	FIELD_DEFAULT("default"),

	//Columns
	FIELD_SOURCENAME("sourceName"),
	FIELD_SCHEMANAME("schemaName"),
	FIELD_TABLENAME("tableName"),
	FIELD_COLUMNNAME("columnName"),
	FIELD_DATATYPE("dataType"),
	FIELD_DATALENGTH("dataLength"),
	FIELD_DATASCALE("dataScale"),
	FIELD_FORMAT("format"),
	FIELD_PRIMARYKEY("primaryKey"),
	FIELD_COLUMNID("columnId")	
	;
	
	private String value;
	private int intValue;
	
	private AvroConstants(final String value) {
		this.value=value;
	}

	private AvroConstants(final int intValue)
	{
		this.intValue=intValue;
	}

	public String getValue() {
		return this.value;
	}
	
	public int getIntValue() {
		return this.intValue;
	}
	
}
