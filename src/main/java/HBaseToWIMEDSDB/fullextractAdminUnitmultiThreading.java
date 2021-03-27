package HBaseToWIMEDSDB;

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
import java.util.Base64.Decoder;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.XML;

import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class fullextractAdminUnitmultiThreading {
	
	public fullextractAdminUnitmultiThreading() {}
	
	private String DBurl, DBusr, DBpwd, HBaseURL, scannerBatch;
	long thisExtractionTS;
	boolean b = true;
	String thisExtractionGV, prevExtractionGV, thisFullExtractionGV; //are long (timestamps) stored string
	
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
	
	//give value to global variables thisExtraction, prevExtraction, thisFullExtraction
	public void setGVExtractionTimes(String ctrlPath) throws IOException {
		////////////
		FileInputStream in = new FileInputStream(ctrlPath);

		Properties props = new Properties();
		props.load(in);
		this.prevExtractionGV = props.getProperty("thisExtraction");
		in.close();

		LocalDateTime now = LocalDateTime.now();
		Timestamp timestamp = Timestamp.valueOf(now);
		thisExtractionTS = timestamp.getTime(); //returns a long
		String current = String.valueOf(thisExtractionTS);
		this.thisExtractionGV = this.thisFullExtractionGV = current;
		///////////	
	}
	
	//set extraction times on control.properties file
	public void setExtractionTimes(String ctrlPath) throws IOException {
		///////////
		FileInputStream in = new FileInputStream(ctrlPath);
		Properties props = new Properties();
		props.load(in);
		String update = props.getProperty("thisFullExtraction");
		long longUpdate = Long.parseLong(update);
		Date d = new Date(longUpdate);
		//System.out.println("last full extraction from AdministrationUnit HBase table was made on: "+ d);
		in.close();

		FileOutputStream out = new FileOutputStream(ctrlPath);
		//set control.properties variables with global var values 
		props.setProperty("prevExtraction", this.prevExtractionGV);
		props.setProperty("thisExtraction", this.thisExtractionGV);
		props.setProperty("thisFullExtraction", this.thisFullExtractionGV);
		long longFullExtractionNEW = Long.parseLong(this.thisExtractionGV);
		Date dNEW = new Date(longFullExtractionNEW);
		System.out.println("NEW full extraction from AdministrationUnit HBase table done at: "+ dNEW);
		props.store(out, null);
		out.close();
		///////////
	}
	
	//returns a JSONArray containing all AdminUnit, encoded
	public JSONArray getDataFromHBase(String scanner_id, String ctrlPath) throws IOException {
		//set extraction times in global variables values, if the load in DB is successful, update in control.properties
		setGVExtractionTimes(ctrlPath);
		System.out.println("...scanner started...");
		JSONArray JSONArrayContent = new JSONArray();
		Integer calls = 0;
		Integer extractedRows = 0;
		Boolean finishScan = false;
		System.out.println("...getting Administration Units from HBase...");

		while (finishScan.equals(false)) {
			OkHttpClient client = new OkHttpClient().newBuilder()
					.build();
			Request request = new Request.Builder()
					.url(HBaseURL+"/scanner/"+scanner_id)
					.method("GET", null)
					.addHeader("Accept", "application/json")
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
				//System.out.println("response status code: "+ statusCode);
			}
			
			else {
				
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
	
	
	public void LoadInDB(String SQLQuery, String DBurl, String DBLogin, String DBPassword) {
		Connection cn = null;
		Statement st = null;	
		System.out.println("...inserting data to DB...");
		try {
			
			//Step 1 : loading driver
			Class.forName("org.postgresql.Driver");
			//Step 2 : Connection
			cn = DriverManager.getConnection(DBurl, DBLogin, DBPassword);
			//Step 3 : creation statement
			st = cn.createStatement();
			// Step 4 execution SQLQuery
			st.executeUpdate(SQLQuery);
			System.out.println("Loading done successfully!");
			
			
			Instant end = Instant.now();
			LocalDateTime ldtStart = LocalDateTime.ofInstant(start, ZoneId.systemDefault());
			LocalDateTime ldtEnd = LocalDateTime.ofInstant(end, ZoneId.systemDefault());
			System.out.println("time spent: " + Duration.between(ldtStart, ldtEnd).getSeconds()+" seconds");
			}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
		}
		finally 
		{
			try 
			{
				cn.close();
				st.close();
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	
	public void setProperties(String ctrlPath) throws IOException{
		///////////
        FileInputStream in = new FileInputStream(ctrlPath);
        
        Properties props = new Properties();
        props.load(in);
		this.setDBurl(props.getProperty("WIMEDSDBurl"));
		this.setDBusr(props.getProperty("WIMEDSDBusr"));
		this.setDBpwd(props.getProperty("WIMEDSDBpwd"));
		this.setHBaseURL(props.getProperty("url"));
		String batch = props.getProperty("scannerbatch");		
		this.scannerBatch = "<Scanner batch="+"\""+batch+"\""+"/>";
		//
		String fe = props.getProperty("thisFullExtraction");
		long longFE = Long.parseLong(fe);
		Date d = new Date(longFE);
		System.out.println("last full extraction from AdministrationUnit HBase table was made on: "+ d);
		//
		
        in.close();
		///////////

	}
    
    
    
    //getters and setters
    
    public void setDBurl(String str)     {this.DBurl = str;}
    public String getDBurl()             {return DBurl;}
    public void setDBusr(String str)     {this.DBusr = str;}
    public String getDBusr()             {return DBusr;}
    public void setDBpwd(String str)     {this.DBpwd = str;}
    public String getDBpwd()             {return DBpwd;}
    
    public void setHBaseURL(String str)     {this.HBaseURL = str;}
    public String getHBaseURL()             {return HBaseURL;}
    
    public void setScannerBatch(String str)     {this.scannerBatch = str;}
    public String getScannerBatch()             {return scannerBatch;}
}
