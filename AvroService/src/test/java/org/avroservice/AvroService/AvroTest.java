package org.avroservice.AvroService;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.avro.Schema;
import org.avroservice.util.AvroUtil;
import org.json.JSONException;

public class AvroTest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws JSONException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException, JSONException, IOException {
		
		final String metaFileName="C:/Users/svaduka/git/SchemaEvolution1/SchemaEvolution/20150317T044228_20156423_CRM_CRMEXTR_ISSUE_FULL.meta";
		Schema oSchema=AvroUtil.convertMetaFileToAVSC(metaFileName);
		System.out.println(oSchema.toString());
		
//		final String dataFileName="C:/Users/svaduka/git/SchemaEvolution1/SchemaEvolution/20150317T044228_20156423_CRM_CRMEXTR_ISSUE_FULL.dat";
//		List<String> lines=FileUtil.readLines(dataFileName);
//		
//		for (int i = 0; i < 10; i++) {
//			GenericRecord record=AvroUtil.createGenericRecordForRow(lines.get(i), oSchema, Boolean.FALSE,"|");
//			System.out.println(record);
//		}

	}

}
