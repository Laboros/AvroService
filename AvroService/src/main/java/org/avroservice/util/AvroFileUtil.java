package org.avroservice.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.avroservice.AvroConstants;
import org.avroservice.pojo.ColumnInfo;
import org.avroservice.pojo.TableMetaData;

import com.commonservice.FileUtil;

public class AvroFileUtil {
	

	public static TableMetaData parseMetaFile(String metaFileNameWithLoc) throws FileNotFoundException, IOException {

		List<String> lines=FileUtil.readLines(metaFileNameWithLoc);
		TableMetaData tblMD = null;
		if(lines!=null && !lines.isEmpty())
		{
				
			final String line=lines.get(AvroConstants.ZERO.getIntValue());
			String[] tokens = StringUtils.splitPreserveAllTokens(line, AvroConstants.SEPERATOR_PIPE.getValue());
			
			tblMD = new TableMetaData();
			
			tblMD.setSOURCE_NAME(!StringUtils.isEmpty(tokens[0])?tokens[0]:AvroConstants.EMPTY.getValue());
			tblMD.setSCHEMA_NAME(!StringUtils.isEmpty(tokens[1])?tokens[1]:AvroConstants.EMPTY.getValue());
			tblMD.setTABLE_NAME(!StringUtils.isEmpty(tokens[2])?tokens[2]:AvroConstants.EMPTY.getValue());
			Set<ColumnInfo> hashSet = new HashSet<ColumnInfo>();
			String strLine=null;
			for (int i=1;i<lines.size();i++) 
			{
				strLine=lines.get(i);
			ColumnInfo columnInfo = AvroFileUtil.createColumnInfo(strLine);
			hashSet.add(columnInfo);
			}
			tblMD.setColumns(hashSet);
			
		}
		return tblMD;
		
	}
	
	public static ColumnInfo createColumnInfo(String line) {
		
		String[] columns = StringUtils.splitPreserveAllTokens(line, "|");
		ColumnInfo columnInfo = null;
		if(!StringUtils.isEmpty(line))
		{
			columnInfo=new ColumnInfo();
				
			columnInfo.setSourceName(!StringUtils.isEmpty(columns[0])?columns[0]:AvroConstants.EMPTY.getValue());
			columnInfo.setSchemaName(!StringUtils.isEmpty(columns[1])?columns[1]:AvroConstants.EMPTY.getValue());
			columnInfo.setTableName(!StringUtils.isEmpty(columns[2])?columns[2]:AvroConstants.EMPTY.getValue());
			columnInfo.setColumnName(!StringUtils.isEmpty(columns[3])?columns[3]:AvroConstants.EMPTY.getValue());
			columnInfo.setDataType(!StringUtils.isEmpty(columns[4])?columns[4]:AvroConstants.EMPTY.getValue());
			columnInfo.setDataLength(!StringUtils.isEmpty(columns[5])?Integer.parseInt(columns[5]):AvroConstants.ZERO.getIntValue());
			columnInfo.setDataScale(!StringUtils.isEmpty(columns[6])?columns[6]:AvroConstants.EMPTY.getValue());
			columnInfo.setFormat(!StringUtils.isEmpty(columns[7])?columns[7]:AvroConstants.EMPTY.getValue());
			columnInfo.setPrimaryKey(StringUtils.isEmpty(columns[8])|| StringUtils.equalsIgnoreCase(columns[8], AvroConstants.FALSE.getValue()) ? Boolean.FALSE:Boolean.TRUE);
			columnInfo.setColumnId(!StringUtils.isEmpty(columns[9])?Integer.parseInt(columns[9]):AvroConstants.ZERO.getIntValue());
									
		}	
		return columnInfo;
	}
}
