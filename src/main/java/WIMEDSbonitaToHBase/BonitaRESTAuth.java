package WIMEDSbonitaToHBase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class BonitaRESTAuth {
	
	public BonitaRESTAuth() {}
	
	
	public String RESTauth() throws IOException {
		
		OkHttpClient client = new OkHttpClient().newBuilder()
				  .build();
				MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded");
				RequestBody body = RequestBody.create("username=admin&password=bpm",mediaType);
				Request request = new Request.Builder()
				  .url("http://localhost:8080/bonita/loginservice")
				  .method("POST", body)
				  .addHeader("Content-Type", "application/x-www-form-urlencoded")
				  .build();
				Response response = client.newCall(request).execute();
				
				
				//just prints JSESSIONID...
				//System.out.println(response.header("Set-Cookie"));
				//System.out.println(response.headers().values("Set-Cookie"));
				String cookie = response.header("Set-Cookie");

				String jsessionid = cookie.substring(11, cookie.indexOf(";"));
				//save JSESSIONID and X-Bonita-API-Token
				    
				String msg = response.message();
				Integer statusCode = response.code();
				System.out.println("response status code: "+ statusCode);
				System.out.println("response msg: "+ msg);
				return jsessionid;
				
	}
	
	//trying with the second call technique (in order to get the token)
	public String getBonitaToken(String jsessionid) throws IOException {

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/system/session/"+jsessionid)
				.method("GET", null)
				.addHeader("Cookie", "JSESSIONID="+jsessionid+";")
				.build();
		Response response = client.newCall(request).execute();

		System.out.println(response.header("Set-Cookie"));
		System.out.println(response.header("X-Bonita-API-Token"));
		return response.header("X-Bonita-API-Token");
	}


	public void setAuthConf(String jsessionid, String bonitaToken, String ctrlPath) throws IOException {

		Properties prop = new Properties();
		
	    try  {
	    	OutputStream output = new FileOutputStream(ctrlPath);
	        // set the properties value
	        prop.setProperty("jsessionid", jsessionid);
	        // save properties to project root folder.
	        prop.store(output, null);
	      } catch (IOException exception) {
	        exception.printStackTrace();
	      } 
		
	}
	

}
