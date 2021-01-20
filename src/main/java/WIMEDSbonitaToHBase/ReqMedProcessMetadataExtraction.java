package WIMEDSbonitaToHBase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class ReqMedProcessMetadataExtraction {

	String jsessionid;
	public ReqMedProcessMetadataExtraction(){}
	
	//get LastUpdated of RequestMedicine Process from WIMEDS(through Bonita bpm REST API)
	public void getLastUpdatedDate(String ctrlPath) throws IOException {
		
		String lastUpdateDate = "";
		//read jsessionid from control.properties for Bonita REST API call
		Properties prop = new Properties();
		
		try {
			InputStream input = new FileInputStream(ctrlPath);
			// load a properties file
			prop.load(input);
			// get the property value and print it out
			jsessionid = prop.getProperty("jsessionid");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		
		try {
		//Bonita REST API call
		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/bpm/process?p=0&c=100&f=name=REQ_RequestMedicines")
				.method("GET", null)
				.addHeader("Cookie", "JSESSIONID=D4687E72BB9628C55F1B2817DC5486D2; X-Bonita-API-Token=55a9b14d-400d-4d04-ac1e-061405bca159;")
				.build();
		Response response = client.newCall(request).execute();
		
		String bodyString = response.body().string();

		JSONArray JSONArray = new JSONArray(bodyString); 
		JSONObject JSONObj = (JSONObject) JSONArray.get(0);
		lastUpdateDate = JSONObj.getString("last_update_date");
		
		setDateTimesProperties(ctrlPath, lastUpdateDate);
		
		}
		catch(Exception e) {
			System.out.println("ERROR: something wrong occured while trying to call Bonita REST API \n---PROCESS STOPPED---");
			System.exit(1);
		}
	}
	
	
	
	public void setDateTimesProperties(String ctrlPath, String lastTime) throws IOException {
		////////////////
        FileInputStream in = new FileInputStream(ctrlPath);

        Properties props = new Properties();
        props.load(in);
        String prevmetadataLastUpdateTime = props.getProperty("metadataLastUpdateTime");
        in.close();
        FileOutputStream out = new FileOutputStream(ctrlPath);

        props.setProperty("metadataLastUpdateTime", lastTime);

        props.store(out, null);
        out.close();
        ////////////

	}
	
	
	public boolean updatemeta(String ctrlPath) throws IOException, ParseException{
		boolean metadataUpdateNeeded = false;
		//get thisWIMEDSextractionDateTime and last_update_date and compare them
		String thisWIMEDSextractionDateTime, last_update_date;
		Properties props = new Properties();
		String metav = "";
		FileInputStream in = new FileInputStream(ctrlPath);
		// load a properties file
		props.load(in);
		// get the property value and print it out

		thisWIMEDSextractionDateTime = props.getProperty("thisWIMEDSextractionDateTime");
		last_update_date = props.getProperty("metadataLastUpdateTime");

		//convertir les dos en el mateix format
		thisWIMEDSextractionDateTime = thisWIMEDSextractionDateTime.substring(0,10)+' '+ thisWIMEDSextractionDateTime.substring(11,23);
		last_update_date = last_update_date.substring(0,10)+' '+ last_update_date.substring(11,23);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date dateMeta = df.parse(last_update_date);
		Date dateData = df.parse(thisWIMEDSextractionDateTime);
		if(dateMeta.after(dateData)) {
			metadataUpdateNeeded=true;
			System.out.println("metadata and metaVersion must be updated");
			//get metadataVersion from config.properties and do metadataVersion+1
			// load a properties file
			props.load(in);
			// get the property value and print it out
			metav = props.getProperty("metadataVersion");
			in.close();

			int metavInt = Integer.parseInt(metav);
			metavInt += 1;
			metav = Integer.toString(metavInt);
			//update metadataVersion on control.properties
			FileOutputStream out = new FileOutputStream(ctrlPath);
			props.setProperty("metadataVersion", metav);
			
			props.store(out, null);
			out.close();

		}

		//i aqui es cridaria a store la metadata pero de momment aixo no ho fem
		else System.out.println("No metadata update needed");
		return metadataUpdateNeeded;
	}
}