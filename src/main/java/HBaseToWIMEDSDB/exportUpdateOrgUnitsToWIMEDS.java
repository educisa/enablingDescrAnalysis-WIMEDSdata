package HBaseToWIMEDSDB;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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

public class exportUpdateOrgUnitsToWIMEDS {
	
	public exportUpdateOrgUnitsToWIMEDS() {}

	private String DBurl, DBusr, DBpwd, HBaseURL, scannerTRbatch, bodyScanner, useMt;
	long thisExtractionTS;
	String thisExtractionGV, prevExtractionGV, thisUpdateGV; //are long stored as string
	
	private String startTime, endTime;
	
	Instant start = Instant.now();
	
	//get scanner id with time range specs
	public String getTRScannerID() throws IOException {

		try {
			OkHttpClient client = new OkHttpClient().newBuilder()
					.build();
			MediaType mediaType = MediaType.parse("text/xml");
			RequestBody body = RequestBody.create(bodyScanner,mediaType);
			Request request = new Request.Builder()
					.url(HBaseURL+"/scanner")
					.method("PUT", body)
					.addHeader("Accept", "text/xml")
					.addHeader("Content-Type", "text/xml")
					.build();
			Response response = client.newCall(request).execute();

			Headers headers = response.headers();
			String scanner_id = headers.value(0);
			//System.out.println("scanner_id: " + scanner_id.substring(scanner_id.lastIndexOf("/") + 1)); 
			scanner_id = scanner_id.substring(scanner_id.lastIndexOf("/") + 1);
			String msg = response.message();
			Integer statusCode = response.code();

			response.body().close();

			return scanner_id;
		}
		catch(Exception e) {System.out.println("Connection error! \nExecution stopped!");
		System.exit(1);
		return null;}
		
	}

	//get all master data rows to be inserted/updated in WIMEDS database
	public JSONArray updateData(String scanner_id) throws IOException {
	
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath + "control.properties";
		//set extraction times in global variables values, if load in DB is successful, update in control.properties
		setGVExtractionTimes(ctrlPath);
		int updatedRows = 0;
		Boolean finishScan = false;
		String SQLQueryUpdate = "";
		Integer calls = 0;
		System.out.println("...starting update scan...");
		JSONArray JSONArrayContent = new JSONArray();
		
		while (finishScan.equals(false)) {
			try {
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
	
			//means scanner has finished
			if(statusCode == 204) {
				response.body().close();
				finishScan=true;
				System.out.println("...update scan finished...");
			}
			
			else {
				JSONObject JSONObjectContent = new JSONObject(content);

				JSONArray JSONArrayRows = (JSONArray) JSONObjectContent.get("Row");
				
				// concatenate arrays
				for (int i = 0; i < JSONArrayRows.length(); i++) {
					++updatedRows;
					JSONArrayContent.put(JSONArrayRows.getJSONObject(i));
				}
			}
			}
			catch(Exception e) {System.out.println("Connection error! \nExecution stopped!");
			System.exit(1);
			}
		}
		System.out.println(updatedRows+" OrgUnits need an update");
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
	
	//give value to global variables thisExtraction, prevExtraction, thisUpdate
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
		this.thisExtractionGV = this.thisUpdateGV = current;
		///////////	
	}
	
	//call this function when extracting data=>update extraction times at props
	public void setExtractionTimes(String ctrlPath) throws IOException {
		///////////
		FileInputStream in = new FileInputStream(ctrlPath);
		Properties props = new Properties();
		props.load(in);
		String update = props.getProperty("thisUpdate");
		long longUpdate = Long.parseLong(update);
		Date d = new Date(longUpdate);
		System.out.println("last update on WIMEDS AdministrationUnit table was made on: "+ d);
		in.close();

		FileOutputStream out = new FileOutputStream(ctrlPath);
		//set control.properties variables with global var values 
		props.setProperty("prevExtraction", this.prevExtractionGV);
		props.setProperty("thisExtraction", this.thisExtractionGV);
		props.setProperty("thisUpdate", this.thisUpdateGV);
		long longUpdateNEW = Long.parseLong(this.thisUpdateGV);
		Date dNEW = new Date(longUpdateNEW);
		System.out.println("NEW update on WIMEDS AdministrationUnit table done at: "+ dNEW);
		props.store(out, null);
		out.close();
		///////////
	}

	
	
	public void LoadInDB(String SQLQuery, String DBurl, String DBLogin, String DBPassword) {
		Connection cn = null;
		Statement st = null;	
		
		try {
			//Step 1 : loading driver
			Class.forName("org.postgresql.Driver");
			//Step 2 : Connection
			cn = DriverManager.getConnection(DBurl, DBLogin, DBPassword);
			//Step 3 : creation statement
			st = cn.createStatement();
			
			// Step 5 execution SQLQuery
			st.executeUpdate(SQLQuery);
			System.out.println("Loading done successfully!");
		
			
			Instant end = Instant.now();
			LocalDateTime ldtStart = LocalDateTime.ofInstant(start, ZoneId.of("CET"));
			LocalDateTime ldtEnd = LocalDateTime.ofInstant(end, ZoneId.of("CET"));
			System.out.println("time spent: " + Duration.between(ldtStart, ldtEnd).getSeconds()+" seconds");
			}
		catch (SQLException e)
		{
			System.out.println("ERROR while loading data into database \nExecution stopped");
			System.exit(1);
		}
		catch (ClassNotFoundException e)
		{
			System.out.println("ERROR:: ClassNotFoundException");
			System.exit(1);
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
    	///////////////
        FileInputStream in = new FileInputStream(ctrlPath);

        Properties props = new Properties();
        props.load(in);
		this.setDBurl(props.getProperty("WIMEDSDBurl"));
		this.setDBusr(props.getProperty("WIMEDSDBusr"));
		this.setDBpwd(props.getProperty("WIMEDSDBpwd"));
		this.setHBaseURL(props.getProperty("url"));
		this.setScannerTRbatch(props.getProperty("batchTR"));
		this.setUseMultiThreading(props.getProperty("useMultithreadingHBaseToWIMEDS"));
		String startTimeCTRL="";
		startTimeCTRL = props.getProperty("thisExtraction");

        in.close();
        
        Instant nowi = Instant.now();
		LocalDateTime now = LocalDateTime.ofInstant(nowi, ZoneId.systemDefault());
        Timestamp timestamp = Timestamp.valueOf(now);
	    long nowTS = timestamp.getTime(); //returns a long
	    
	    String endTimeCTRL = String.valueOf(nowTS);
        String scannerBatchString = "<Scanner batch="+"\""+scannerTRbatch+"\"";
        String startTimeString = " startTime="+"\""+startTimeCTRL+"\"";
        String endTimeString = " endTime="+"\""+endTimeCTRL+"\""+" />";
        //body <= scannerBatchString, startTimeString, endTimeSTring
        this.bodyScanner = scannerBatchString+startTimeString+endTimeString;
    	//////////////
    }
	
	
	
	
    //getters and setters
    
    public void setDBurl(String str)     {this.DBurl = str;}
    public String getDBurl()             {return DBurl;}
    public void setDBusr(String str)     {this.DBusr = str;}
    public String getDBusr()             {return DBusr;}
    public void setDBpwd(String str)     {this.DBpwd = str;}
    public String getDBpwd()             {return DBpwd;}
    
    public void setScannerTRbatch(String str)     {this.scannerTRbatch = str;}
    public String getScannerTRbatch()             {return scannerTRbatch;}
    
    public void setHBaseURL(String str)     {this.HBaseURL = str;}
    public String getHBaseURL()             {return HBaseURL;}
    
    public void setUseMultiThreading(String str)     {this.useMt = str;}
    public String getUseMultiThreading()             {return useMt;}
    
}
