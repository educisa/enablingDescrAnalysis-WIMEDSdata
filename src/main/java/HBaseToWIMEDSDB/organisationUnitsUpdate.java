package HBaseToWIMEDSDB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Base64.Decoder;
import java.util.Date;

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

public class organisationUnitsUpdate {
public organisationUnitsUpdate() {}
	
	private String DBurl, DBusr, DBpwd, HBaseURL, scannerTRbatch, bodyScanner;
	long thisExtractionTS;
	String thisExtractionGV, prevExtractionGV, thisUpdateGV; //are long stored as string
	
	private String startTime, endTime;
	
	Instant start = Instant.now();
	
	
	public String getScannerId() throws UnsupportedEncodingException, IOException{
		OkHttpClient client = new OkHttpClient().newBuilder()
				  .build();
				MediaType mediaType = MediaType.parse("String");
				
				RequestBody body = RequestBody.create(bodyScanner, mediaType);
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
				System.out.println("response status code: "+ statusCode);
				System.out.println("response msg: "+ msg);
				response.body().close();
				
				return scanner_id;
	}
	
	
	public String getTRScannerID() throws IOException {
		
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
				System.out.println("scanner_id: " + scanner_id.substring(scanner_id.lastIndexOf("/") + 1)); 
				scanner_id = scanner_id.substring(scanner_id.lastIndexOf("/") + 1);
				String msg = response.message();
				Integer statusCode = response.code();
				//System.out.println("response status code: "+ statusCode);
				//System.out.println("response msg: "+ msg);
				response.body().close();
				
				return scanner_id;
	}
    
    
	
	public String updateData(String scanner_id) throws IOException {

		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath + "control.properties";
		//set extraction times in global variables values, if load in DB is successful, update in control.properties
		setGVExtractionTimes(ctrlPath);
		int updatedRows = 0;
		Boolean finishScan = false;
		String SQLQueryUpdate = "";
		Integer calls = 0;
		System.out.println("...starting update scan...");
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

			//means scanner has finished
			if(statusCode == 204) {
				response.body().close();
				finishScan=true;
				System.out.println("...update scan finished...");
				System.out.println(updatedRows+" rows updated");
			}
			//else statusCode = 200 and everything is OK
			else {
				JSONObject JSONObjectContent = new JSONObject(content);

				JSONArray JSONArrayRows = (JSONArray) JSONObjectContent.get("Row");

				// concatenate arrays
				for (int i = 0; i < JSONArrayRows.length(); i++) {
					++updatedRows;
					//JSONArrayContent.put(JSONArrayRows.getJSONObject(i));
					//generate query for each adminunit between the range low,high
					JSONObject AdminUnitObj = JSONArrayRows.getJSONObject(i);
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


					SQLQueryUpdate += "UPDATE administrationunit SET parentid='"
							+parentid + "',"
							+"name='"
							+name.replaceAll("'","''") + "',"
							+"shortName='"
							+shortname.toString().replaceAll("'","''") + "',"
							+"datelastupdated='"
							+datelastupdated.substring(0,10) + "',"
							+"leaf="
							+leaf + ","
							+"levelnumber="
							+levelnumber+","
							+"address='"
							+address+"',"
							+"url='"
							+url+"'"
							+" WHERE id='"+id+"';\n";
				}
			}
			response.body().close();
		}

		deleteScanner(scanner_id);
		return SQLQueryUpdate;	
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
			System.out.println("time spent: " + Duration.between(ldtStart, ldtEnd).toSeconds()+" seconds");
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
    

}

