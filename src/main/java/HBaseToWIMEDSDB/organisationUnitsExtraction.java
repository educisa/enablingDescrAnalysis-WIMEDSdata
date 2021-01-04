package HBaseToWIMEDSDB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
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
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
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

public class organisationUnitsExtraction {

public organisationUnitsExtraction() {}
	
	private String DBurl, DBusr, DBpwd, HBaseURL, scannerBatch;
	long thisExtractionTS;
	boolean b = true;
	
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
	
	//call this function when extracting data=>update extraction times at props
	public void setExtractionTimes(String ctrlPath) throws IOException {
		////////////
		FileInputStream in = new FileInputStream(ctrlPath);

		Properties props = new Properties();
		props.load(in);
		String prevExtraction = props.getProperty("thisExtraction");
		in.close();

		FileOutputStream out = new FileOutputStream(ctrlPath);

		LocalDateTime now = LocalDateTime.now();
		Timestamp timestamp = Timestamp.valueOf(now);
		thisExtractionTS = timestamp.getTime(); //returns a long
		String current = String.valueOf(thisExtractionTS);
		props.setProperty("prevExtraction", prevExtraction);
		props.setProperty("thisExtraction", current);

		props.store(out, null);
		out.close();
		//////////
	}
	
	
	//sense paremtriztar attr amb tots GETs
	public String getDataFromHBase(String scanner_id, String ctrlPath) throws IOException {
		System.out.println("...scanner started...");
		Integer calls = 0;
		Integer extractedRows = 0;
		String SQLQuery = "TRUNCATE TABLE AdministrationUnit;\n";
		Boolean finishScan = false;
		System.out.println("...generating SQLQuery from HBase data...");

		while (finishScan.equals(false)) {
			OkHttpClient client = new OkHttpClient().newBuilder()
					.build();
			Request request = new Request.Builder()
					.url(HBaseURL+"/scanner/"+scanner_id)
					.method("GET", null)
					.addHeader("Accept", "text/xml")
					.build();
			Response response = client.newCall(request).execute();
			ResponseBody body = response.body();
			++calls;
			String encodedBodyxml = response.body().string();
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
				JSONObject xmlJSONObj = XML.toJSONObject(encodedBodyxml);
				JSONObject CellSetJSONObj = (JSONObject) xmlJSONObj.get("CellSet");
				//get the row as a string
				String stringRow = CellSetJSONObj.get("Row").toString();
				
				Object json = new JSONTokener(stringRow).nextValue();
				
				if(json instanceof JSONObject) {
					
					JSONObject RowSetJSONobj = (JSONObject) CellSetJSONObj.get("Row");
					
					++extractedRows;

					//getting the content
					String content = RowSetJSONobj.getJSONObject("Cell").getString("content");

					//decoding
					Decoder decoder = Base64.getDecoder();
					byte[] bytes = decoder.decode(content);
					String decodedContent = new String(bytes, "UTF-8");
					JSONObject resultContentJSON = new JSONObject(decodedContent);
					
					String url = null;//just some organisationUnits have (e.g., http://www.hospitalclinic.org)
					if(resultContentJSON.has("url")) {
						url = resultContentJSON.getString("url");
						url = url.replaceAll("'","''");
					}
					
					String address = null;//just some organisationUnits have (e.g., calle Villarroel, 170, 08036 Barcelona, Spain)
					if(resultContentJSON.has("address")) {
						address = resultContentJSON.getString("address");
						address = address.replaceAll("'","''");
					}
					
					String parentid = "";

					if(resultContentJSON.has("parent")) {
						JSONObject JSONObjparent = resultContentJSON.getJSONObject("parent");
						parentid = JSONObjparent.getString("id");
					}

					String id, name, shortname, datelastupdated;
					Boolean leaf;
					Integer levelnumber;
					id = resultContentJSON.getString("id");
					name = resultContentJSON.getString("name");
					shortname = resultContentJSON.getString("shortName");
					datelastupdated = resultContentJSON.getString("lastUpdated");
					leaf = resultContentJSON.getBoolean("leaf");
					levelnumber = resultContentJSON.getInt("level");


					SQLQuery += "INSERT INTO administrationunit VALUES ('"
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
				
				else if(json instanceof JSONArray) {
					
					JSONArray resultsJArray1 = new JSONArray();
					resultsJArray1 = CellSetJSONObj.optJSONArray("Row");
					Integer JSONlength = resultsJArray1.length();
					
					for(int i = 0; i < JSONlength; ++i) {
						JSONObject rowJSONObj = (JSONObject) resultsJArray1.get(i);

						++extractedRows;
						
						//getting the content
						String content = rowJSONObj.getJSONObject("Cell").getString("content");
						
				        byte[] dataBytes = Base64.getDecoder().decode(content);
				        String decodedContent = new String(dataBytes, StandardCharsets.UTF_8.name());
				        JSONObject resultContentJSON = new JSONObject(decodedContent);
						
						
						String parentid = "";
						if(resultContentJSON.has("parent")) {
							JSONObject JSONObjparent = resultContentJSON.getJSONObject("parent");
							parentid = JSONObjparent.getString("id");
						}
						
						String url = null;//just some organisationUnits have (e.g., http://www.hospitalclinic.org)
						if(resultContentJSON.has("url")) {
							url = resultContentJSON.getString("url");
							url = url.replaceAll("'","''");
						}
						
						String address = null;//just some organisationUnits have (e.g., calle Villarroel, 170, 08036 Barcelona, Spain)
						if(resultContentJSON.has("address")) {
							address = resultContentJSON.getString("address");
							address = address.replaceAll("'","''");
						}
						
						String id, name, shortname, datelastupdated;
						Boolean leaf;
						Integer levelnumber;
						id = resultContentJSON.getString("id");
						name = resultContentJSON.getString("name");
						shortname = resultContentJSON.getString("shortName");
						datelastupdated = resultContentJSON.getString("lastUpdated");
						leaf = resultContentJSON.getBoolean("leaf");
						levelnumber = resultContentJSON.getInt("level");
						
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
				}
			}
			response.body().close();
		}
		System.out.println(calls + " scanner calls done");
		System.out.println(extractedRows+" rows extracted");
		deleteScanner(scanner_id);
		//update extraction times after doing the extraction
		setExtractionTimes(ctrlPath);
		System.out.println("...QUERY ready...");
		return SQLQuery; 
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
	
	
	
	public void setProperties(String ctrlPath) throws IOException{
		/////////////
	    FileInputStream in = new FileInputStream(ctrlPath);

        Properties props = new Properties();
        props.load(in);
		this.setDBurl(props.getProperty("DBurl"));
		this.setDBusr(props.getProperty("DBusr"));
		this.setDBpwd(props.getProperty("DBpwd"));
		this.setHBaseURL(props.getProperty("url"));
		this.setScannerBatch(props.getProperty("batch"));
        in.close();
		////////////

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

