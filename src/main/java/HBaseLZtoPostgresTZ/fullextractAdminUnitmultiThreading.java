package HBaseLZtoPostgresTZ;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Date;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class fullextractAdminUnitmultiThreading {
public fullextractAdminUnitmultiThreading() {}
	
	private String HBaseURL, scannerBatch;
	long thisExtractionTS;
	boolean b = true;
	String thisExtractionGV, prevExtractionGV, thisFullExtractionGV, threadControl; //are long (timestamps) stored string
	
	Instant start = Instant.now();
	

	public String getScannerId() throws UnsupportedEncodingException, IOException{
		OkHttpClient client = new OkHttpClient().newBuilder()
				  .build();
				MediaType mediaType = MediaType.parse("String");
				
				RequestBody body = RequestBody.create(scannerBatch, mediaType);
				Request request = new Request.Builder()
				  .url(HBaseURL+"/scanner")
				  .method("PUT", body)
				  .addHeader("Accept", "text/xml")
				  .addHeader("Content-Type", "text/xml")
				  .build();
				
				Response response = client.newCall(request).execute();
				
				Headers headers = response.headers();
				String scanner_id = headers.value(0);
				System.out.println("scanner_id: " + scanner_id.substring(scanner_id.lastIndexOf("/") + 1)); 
				scanner_id = scanner_id.substring(scanner_id.lastIndexOf("/") + 1);
				String msg = response.message();
				Integer statusCode = response.code();
				
				response.body().close();
				
				return scanner_id;
	}
	
	//returns a JSONArray containing all AdminUnit, encoded
	public JSONArray getDataFromHBase(String scanner_id) throws IOException {
		
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath + "control.properties";
		//set extraction times in global variables values, if the load in DB is successful, update in control.properties
		setGVExtractionTimes(ctrlPath);
		System.out.println("...scanner started...");
		JSONArray JSONArrayContent = new JSONArray();
		Integer calls = 0;
		Integer extractedRows = 0;
		Boolean finishScan = false;
		System.out.println("...getting Administration Units from HBase Table...");

		while (finishScan.equals(false)) {
			OkHttpClient client = new OkHttpClient().newBuilder()
					.build();
			Request request = new Request.Builder()
					.url(HBaseURL+"/scanner/"+scanner_id)
					.method("GET", null)
					.addHeader("Accept", "application/json")//em retornara el Row[]
					.build();
			Response response = client.newCall(request).execute();
			ResponseBody body = response.body();
			++calls;
			String content = response.body().string();
			Headers headers = response.headers();

			String msg = response.message();
			Integer statusCode = response.code();

			if(statusCode != 200) {
				response.body().close();
				finishScan=true;
				System.out.println("...scanner finsihed...");
				System.out.println("response status code: "+ statusCode);
			}
			
			else {

				//agafem el Row[], esta a dins d'un JSONObject gegant {}
				JSONObject JSONObjectContent = new JSONObject(content);

				JSONArray JSONArrayRows = (JSONArray) JSONObjectContent.get("Row");
				
				// concatenate arrays
				for (int i = 0; i < JSONArrayRows.length(); i++) {
					++extractedRows;
					JSONArrayContent.put(JSONArrayRows.getJSONObject(i));
				}
			}
		}
		System.out.println("extractedRows: "+extractedRows);
		deleteScanner(scanner_id);
		return JSONArrayContent;
	}
			

	
	//return the complete SQLQUery for AdministrationUnit
	public String getSQLQueryAdminUnit() throws IOException, InterruptedException {
		//getting data from Administration Units from DEV-Org-Units Table landing zone HBase
		String scanner_id = getScannerId();
		JSONArray content = getDataFromHBase(scanner_id);
		int THREADS;
		if(threadControl.equals("availableProcessors")) THREADS =  Runtime.getRuntime().availableProcessors();
		else THREADS = Integer.parseInt(threadControl);
		System.out.println("using "+THREADS+" threads, generating queries");
		//call ParallelPartialQueryGenerator
		ParallelPartialQueryGenerator partialQueryGen = new ParallelPartialQueryGenerator(THREADS);
		String OrgUnitSQLQuery = "TRUNCATE TABLE AdministrationUnit;\n";
		//put workers to work and get the result query 
		OrgUnitSQLQuery += partialQueryGen.partialQueriesSum(content, "AdministrationUnit");
		return OrgUnitSQLQuery;
	}
	
	//generating SQLQuery not using using multithreading
		public String getSQLQueryAdminUnitnoMT() throws IOException {
			//getting data from Administration Units from DEV-Org-Units Table landing zone HBase
			String scanner_id = getScannerId();
			JSONArray content = getDataFromHBase(scanner_id);
			String SQLQuery="";
			System.out.println("length del content sense utilitzar MULtithtreading: " + content.length());
			for(int i =0; i < content.length(); ++i) {
				//generate query for each adminunit between the range low,high
				JSONObject AdminUnitObj = content.getJSONObject(i);
				JSONArray encodedJSONArray = AdminUnitObj.getJSONArray("Cell");
				JSONObject encodedJSONObject = encodedJSONArray.getJSONObject(0);
				String encodedValue = encodedJSONObject.getString("$");
				//decode the encodedValue
				byte[] dataBytes = Base64.getDecoder().decode(encodedValue);
				String decodedValue="";
				try {
					decodedValue = new String(dataBytes, StandardCharsets.UTF_8.name());
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				//transform it to JSON to get the fields we are interested in
				JSONObject resultJSONValue = new JSONObject(decodedValue);


				String parentid = "";
				if(resultJSONValue.has("parent")) {
					JSONObject JSONObjparent = resultJSONValue.getJSONObject("parent");
					parentid = JSONObjparent.getString("id");
				}

				String url = null;//just some organisationUnits have (e.g., http://www.hospitalclinic.org)
				if(resultJSONValue.has("url")) {
					url = resultJSONValue.getString("url");
					url = url.replaceAll("'","''");
				}

				String address = null;//just some organisationUnits have (e.g., calle Villarroel, 170, 08036 Barcelona, Spain)
				if(resultJSONValue.has("address")) {
					address = resultJSONValue.getString("address");
					address = address.replaceAll("'","''");
				}

				String id, name, shortname, datelastupdated;
				Boolean leaf;
				Integer levelnumber;
				id = resultJSONValue.getString("id");
				name = resultJSONValue.getString("name");
				shortname = resultJSONValue.getString("shortName");
				datelastupdated = resultJSONValue.getString("lastUpdated");
				leaf = resultJSONValue.getBoolean("leaf");
				levelnumber = resultJSONValue.getInt("level");

				SQLQuery += "INSERT INTO AdministrationUnit VALUES ('"
						+ id + "','"
						+ parentid + "','"
						+ name.replaceAll("'","''") + "','"
						+ shortname.toString().replaceAll("'","''") + "','"
						+ datelastupdated.substring(0,10) + "',"
						+ leaf + ","
						+ levelnumber + ",'"
						+ address + "','"
						+ url + "');\n";
			}
			
			return SQLQuery;
		}
	
	
	//call this function when extracting data=>update extraction times at props
	public void setExtractionTimes(String ctrlPath) throws IOException {
		///////////
        FileInputStream in = new FileInputStream(ctrlPath);
        
        Properties props = new Properties();
        props.load(in);
        String prevExtraction = props.getProperty("tz_thisExtractionOrgUnits");
        in.close();
        
        FileOutputStream out = new FileOutputStream(ctrlPath);

        LocalDateTime now = LocalDateTime.now();
		Timestamp timestamp = Timestamp.valueOf(now);
		thisExtractionTS = timestamp.getTime(); //returns a long
		String current = String.valueOf(thisExtractionTS);
		props.setProperty("tz_prevExtractionOrgUnits", prevExtraction);
		props.setProperty("tz_thisExtractionOrgUnits", current);
		props.setProperty("tz_lastFullExtractionOrgUnits", current);

        props.store(out, null);
        out.close();
		//////////
	}
	
	//give value to global variables thisExtraction, prevExtraction, thisFullExtraction
	public void setGVExtractionTimes(String ctrlPath) throws IOException {
		////////////
		FileInputStream in = new FileInputStream(ctrlPath);

		Properties props = new Properties();
		props.load(in);
		this.prevExtractionGV = props.getProperty("tz_thisExtractionOrgUnits");
		in.close();

		LocalDateTime now = LocalDateTime.now();
		Timestamp timestamp = Timestamp.valueOf(now);
		thisExtractionTS = timestamp.getTime(); //returns a long
		String current = String.valueOf(thisExtractionTS);
		this.thisExtractionGV = this.thisFullExtractionGV = current;
		///////////	
	}
	
	
	//delete scanner after using it
	public void deleteScanner(String scanner_id) throws IOException {
		OkHttpClient client = new OkHttpClient().newBuilder()
				  .build();
				MediaType mediaType = MediaType.parse("text/plain");
				RequestBody body = RequestBody.create("", mediaType);
				Request request = new Request.Builder()
				  .url(HBaseURL+"/scanner/"+scanner_id)
				  .method("DELETE", body)
				  .addHeader("Accept", "text/xml")
				  .build();
				Response response = client.newCall(request).execute();
				response.body().close();
	}
	
	
	public void setProperties(String ctrlPath) throws IOException{
		///////////
        FileInputStream in = new FileInputStream(ctrlPath);
        
        Properties props = new Properties();
        props.load(in);
		this.setHBaseURL(props.getProperty("url"));
		this.setScannerBatch(props.getProperty("batch"));	
		this.setThreadControl(props.getProperty("nThreads"));
		//
		String fe = props.getProperty("tz_thisExtractionOrgUnits");
		long longFE = Long.parseLong(fe);
		Date d = new Date(longFE);
		System.out.println("last full extraction from AdministrationUnit HBase table to transformedZone was made on: "+ d);
		//
		
        in.close();
		///////////

	}
    
    //getters and setters
    
    public void setHBaseURL(String str)     {this.HBaseURL = str;}
    public String getHBaseURL()             {return HBaseURL;}
    
    public void setScannerBatch(String str)     {this.scannerBatch = str;}
    public String getScannerBatch()             {return scannerBatch;}
    
	public String getThreadControl() {return this.threadControl;}
	public void setThreadControl(String s) {this.threadControl = s;}
}
