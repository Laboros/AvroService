package org.avroservice.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.avroservice.AvroConstants;
import org.avroservice.pojo.ColumnInfo;
import org.avroservice.pojo.TableMetaData;

public class FileUtil {
	
	public static List<String> readLines(String fileNameWithLoc) throws IOException{
		
		List<String> lines = FileUtils.readLines(new File(fileNameWithLoc));
		return lines;
		
	}

	public static TableMetaData parseMetaFile(String metaFileNameWithLoc) throws FileNotFoundException, IOException {

		List<String> lines=readLines(metaFileNameWithLoc);
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
			ColumnInfo columnInfo = FileUtil.createColumnInfo(strLine);
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
	

	public static List<File> getTriggerFiles(final String inboxLoc, final String triggerExt)
	{
		String[] extensions = new String[] {triggerExt};
		List<File> files = (List<File>)FileUtils.listFiles(new File(inboxLoc), extensions, true);
		return files; 
	}
	
	/**
	 * 
	 * @param triggerFileNameWithLocAndExt  D:\Naveen\Datasets\SchemaEvolution\20150317T044228_20156423_ORS_ORSEXTR_ISSUE_FULL.ctl
	 * @param triggerExt ctl 
	 * lookupName = 
	 * @return
	 */
	
	public static List<String> getSimilarFiles(final String triggerFileNameWithLocAndExt, final String triggerExt)
	{
		List<String> similarFiles=new ArrayList<String>();
		
		final String lookUpName=getFileName(triggerFileNameWithLocAndExt);
		
		System.out.println("LookUpName:"+lookUpName);
		
		File f=new File(triggerFileNameWithLocAndExt);
		
		File parentFile=f.getParentFile();
		
		File[] files=parentFile.listFiles(new FilenameFilter() {
			
			public boolean accept(File dir, String name) {
				// TODO Auto-generated method stub
				return StringUtils.indexOf(name, lookUpName)!=-1;
			}
		});
		
		for (File file : files) {
			similarFiles.add(file.getAbsolutePath());
		}
		
		return similarFiles;
		
	}
	
	public static Map<String, List<String>> groupSimilarFiles(final String inbox_loc,final String triggerFileExt){
		
		List<File> triggeFiles=getTriggerFiles(inbox_loc, triggerFileExt);
		
		Map<String, List<String>> groupFiles=null;
		
		if(triggeFiles!=null && !triggeFiles.isEmpty())
		{
			groupFiles=new HashMap<String, List<String>>();
			
			for (File file : triggeFiles) {
				final String key=getFileName(file.getAbsolutePath());
				List<String> similarFiles=getSimilarFiles(file.getAbsolutePath(), triggerFileExt);
				groupFiles.put(key, similarFiles);
			}
		}
		return groupFiles;
	}
	
	public static String getFileName(final String fileNameWithExt)
	{
		String onlyFileName=fileNameWithExt.substring(fileNameWithExt.lastIndexOf(AvroConstants.FILE_SEPARATOR.getValue())+1);
		if(onlyFileName==null){
			onlyFileName=fileNameWithExt;
		}
		onlyFileName=onlyFileName.substring(0,onlyFileName.lastIndexOf("."));
		
		return onlyFileName;
	}
	
	
	public boolean moveToArchiveFile(final String inbox_loc,final String archive_loc, final String moveFileLookupName)
	{
		return Boolean.FALSE;
	}
	
	public static String getExtFile(final List<String> groupFiles, final String extFile)
	{
		String ctlFile=null;
		for (String file : groupFiles) {
			if(file.indexOf(("."+extFile))!=-1){
				ctlFile=file;
				break;
			}
		}
		return ctlFile;
	}
	
	public static String getExtFile(final String anyFileNameWithExt, final String extFile)
	{
		final String onlyFileName=getFileName(anyFileNameWithExt);
	
		return onlyFileName.concat(extFile);
	}

	public static void moveFileToLoc(String fileNameWithLoc,String destinationLoc) throws IOException {
		FileUtils.moveFileToDirectory(new File(fileNameWithLoc), new File(destinationLoc), Boolean.TRUE);
		
	}
	
}
