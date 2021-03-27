package HBaseToWIMEDSDB;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Base64;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class modifyingHBaseRows {


	public static void main(String[] args) throws IOException, Exception{
		
		String id = getScannerId();
		String orgunit = getDataFromHBase(id);
		
		putDatatoHBase(orgunit);
	}
	
	public static String getScannerId() throws UnsupportedEncodingException, IOException{
		OkHttpClient client = new OkHttpClient().newBuilder()
				  .build();
				MediaType mediaType = MediaType.parse("String");
				String scannerBatch = "<Scanner batch="+"\""+2+"\""+"/>";
				RequestBody body = RequestBody.create(scannerBatch, mediaType);
				Request request = new Request.Builder()
				  .url("http://who-dev.essi.upc.edu:60000/WHO-DEV-OrgUnits"+"/scanner")
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
	
	public static String getDataFromHBase(String scanner_id) throws IOException {
		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WHO-DEV-OrgUnits"+"/scanner/"+scanner_id)
				.method("GET", null)
				.addHeader("Accept", "application/json")
				.build();
		Response response = client.newCall(request).execute();
		ResponseBody body = response.body();
		String content = response.body().string();
		Headers headers = response.headers();

		String msg = response.message();
		Integer statusCode = response.code();

		JSONObject JSONObjectContent = new JSONObject(content);
		System.out.println(JSONObjectContent);
		JSONArray JSONArrayRows = (JSONArray) JSONObjectContent.get("Row");
		System.out.println(JSONArrayRows);
		JSONObject jo = (JSONObject) JSONArrayRows.get(0);
		System.out.println(jo);
		JSONArray ja = (JSONArray) jo.get("Cell");
		System.out.println(ja);
		JSONObject jo1 = (JSONObject) ja.get(0);
		System.out.println(jo1);
		String val = (String) jo1.get("$");
		System.out.println(val); //already encoded
		
		///////////////////////////////
		String stringKey = "/Akorabo";
		String stringColumn = "organisationUnits:organisationUnits";

		Base64.Encoder enc = Base64.getEncoder();

		String encodedKey = enc.encodeToString(stringKey.getBytes());

		String encodedColumn = enc.encodeToString(stringColumn.getBytes());

		encodedKey = '"'+encodedKey+'"';
		encodedColumn = '"'+encodedColumn+'"';

		String JSONrowPUT = "{\"Row\":[{\"key\":"+encodedKey+", \"Cell\": [{\"column\":"+encodedColumn+", \"$\":"+val+"}]}]}";

		System.out.println(JSONrowPUT);
		//return JSONObjectContent.toString();
		return JSONrowPUT;
	}
	
	
	public static void putDatatoHBase(String json) throws IOException {

		//encode data for HBase
		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		MediaType mediaType = MediaType.parse("application/json");
		RequestBody body = RequestBody.create(json, mediaType);
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WHO-DEV-OrgUnits/fakerow")
				.method("PUT", body)
				.addHeader("Content-Type", "application/json")
				.addHeader("Accept", "application/json")
				.build();
		Response response = client.newCall(request).execute();

		String msg = response.message();
		Integer statusCode = response.code();
		
		System.out.println(msg);
		System.out.println(statusCode);
	}
	}
	
