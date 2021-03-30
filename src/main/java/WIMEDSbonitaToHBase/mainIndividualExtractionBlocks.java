package WIMEDSbonitaToHBase;

import java.io.IOException;

public class mainIndividualExtractionBlocks {

	public mainIndividualExtractionBlocks() {}
	
	public static void main(String[] args) throws IOException, Exception{
		
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath + "control.properties";
		
		//falta HBBaseUtilities per si s'ha de crear la taula WIMEDS-Table-Data, etc.
		HbaseUtilities hbase = new HbaseUtilities(ctrlPath);
		hbase.createDataTable();
		
		//call to RESTAuth and save JSESSIONID and X-Bonita-API-Token in control.properties (BonitaRESTAuth.java)
		
		//cridar aviam si es necessita agafar també la metadata.
		//tinc implementat el codi per actualitzar la rowKey si es necessita metadata update.
		/*cada cop que es faci extraction/update de la data mirar si es necessari o no extreure metadata from the RequestMedicini process
		 * fer crida a bpm REST API de Bonita per que em proporcioni el lastUpdated
		 * si lastUpdated=>thisWIMEDSextractionDateTime llavors s'ha d'extreure metadata i data(canviar la metadataVersion i DateTime a la key)
		 * si lastUpdated<thisWIMEDSextractionDateTime llavors nomÃ©s s'extreu la data(i nomÃ©s canvian el DateTime de la key).
		 */
		
		ReqMedProcessMetadataExtraction rm= new ReqMedProcessMetadataExtraction();
		rm.getLastUpdatedDate(ctrlPath);
		boolean metadataUpdateNeeded = rm.updatemeta(ctrlPath);
		//un cop fet això ja tenim actualitzada la metadataversion a control.properties
		//if metadataUpdateNeeded llavors hauriem de cridar a la funció (NO IMPLMENTED) que exportes la metadata a HBase.
		
		
		IndividualExtractionBlocks ire = new IndividualExtractionBlocks();
		ire.setProps(ctrlPath);
		
		//export WIMEDS data to HBase 
		ire.exportData();
		//update extraction Date Time, it should be updated with a GV like I do in the other extractions
		ire.updateWIMEDSextractionDateTime(ctrlPath);
	}
	
}
