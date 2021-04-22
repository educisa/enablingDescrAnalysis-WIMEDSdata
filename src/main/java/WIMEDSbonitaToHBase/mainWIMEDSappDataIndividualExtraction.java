package WIMEDSbonitaToHBase;

import java.io.IOException;

public class mainWIMEDSappDataIndividualExtraction {

	public mainWIMEDSappDataIndividualExtraction() {}
	
	public static void main(String[] args) throws IOException, Exception{
		
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath + "control.properties";
		
		HbaseUtilities hbase = new HbaseUtilities(ctrlPath);
		hbase.createDataTable();
		
		//call to RESTAuth and save JSESSIONID and X-Bonita-API-Token in control.properties (BonitaRESTAuth.java)
		
		//make a call to know if a metadata update is needed
		//the code implements the update of the rowKey if an update is needed.
		/*each time a extraction/update of the WIMEDS application data is done-> look if its necessary to extract again a RequestMedicine process metadata
		 * call bpm REST API to get lastUpdated
		 * if lastUpdated=>thisWIMEDSextractionDateTime then s'ha d'extreure metadata i data(change metadataVersion and DateTime from the key).
		 * if lastUpdated<thisWIMEDSextractionDateTime then we just extract datajust the DateTime of the rowKey is modified).
		 */
		ReqMedProcessMetadataExtraction rm= new ReqMedProcessMetadataExtraction();
		rm.getLastUpdatedDate(ctrlPath);
		boolean metadataUpdateNeeded = rm.updatemeta(ctrlPath);
		// metadataversion updated in control.properties
		//if metadataUpdateNeeded then we should call a function extracting the metadata exporting it to HBase (NO IMPLMENTED).
		
		WIMEDSappDataIndividualExtraction ire = new WIMEDSappDataIndividualExtraction();
		ire.setProps(ctrlPath);
		
		//export WIMEDS data to HBase 
		ire.exportData();
		//update extraction Date Time, it should be updated with a GV like I do in the other extractions
		ire.updateWIMEDSextractionDateTime(ctrlPath);
	}
	
}
