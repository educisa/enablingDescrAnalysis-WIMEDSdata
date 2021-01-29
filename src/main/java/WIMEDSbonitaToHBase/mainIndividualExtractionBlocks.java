package WIMEDSbonitaToHBase;

import java.io.IOException;

public class mainIndividualExtractionBlocks {

	public mainIndividualExtractionBlocks() {}
	
	public static void main(String[] args) throws IOException, Exception{
		
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath + "control.properties";
		
		IndividualExtractionBlocks ire = new IndividualExtractionBlocks();
		ire.setProps(ctrlPath);
		
		//export WIMEDS data to HBase 
		ire.exportData();
		//update extraction Date Time, it should be updated with a GV like I do in the other extractions
		ire.updateWIMEDSextractionDateTime(ctrlPath);
	}
	
}
