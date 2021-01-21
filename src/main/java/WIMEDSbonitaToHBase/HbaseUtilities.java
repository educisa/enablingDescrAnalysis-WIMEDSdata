package WIMEDSbonitaToHBase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class HbaseUtilities {
	
	String tableName, family;
	
	 public HbaseUtilities(String file){
	        this.setProperties(file);
	 }
	 
	 
	 public void createDataTable() throws IOException {
		 //primer mirem que no existeixi aquella taula, es podria fer agafant el resultat de la crida:
		 
		 try {
		 OkHttpClient client = new OkHttpClient().newBuilder()
				 .build();
		 Request request = new Request.Builder()
				 .url("http://who-dev.essi.upc.edu:60000/")
				 .method("GET", null)
				 .build();
		 Response response = client.newCall(request).execute();

		 ResponseBody body = response.body();
		 String tablesHBase = response.body().string();
		 System.out.println(tablesHBase);



		 OkHttpClient client1 = new OkHttpClient().newBuilder()
				 .build();
		 Request request1 = new Request.Builder()
				 .url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/schema")
				 .method("GET", null)
				 .build();
		 Response response1 = client1.newCall(request1).execute();

		 Integer statusCode1 = response1.code();
		 if(statusCode1==200) {
			 System.out.println(this.tableName+" already exists");
		 }
		 else {
			 String tableNameaux = '"'+tableName+'"';;
			 String familyaux = '"'+family+'"';
			 
			 OkHttpClient client2 = new OkHttpClient().newBuilder()
					 .build();
			 MediaType mediaType = MediaType.parse("text/xml");
			 RequestBody body2 = RequestBody.create(mediaType, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><TableSchema name="+tableNameaux+"><ColumnSchema name="+familyaux+"/></TableSchema>");
			 Request request2 = new Request.Builder()
					 .url("http://who-dev.essi.upc.edu:60000/"+tableName+"/schema")
					 .method("POST", body2)
					 .addHeader("Accept", "text/xml")
					 .addHeader("Content-Type", "text/xml")
					 .build();
			 Response response2 = client2.newCall(request2).execute();
			 Integer statusCode2 = response2.code();
			 String msg2 = response2.message();
			 
			 System.out.println(tableName +" created.");
		 }
		 }
		 catch(Exception e) {
			 System.out.println("ERROR: something went wrong with HBase instance connection");
		 }
	 }
	 
	 
	 public void addColumnFamily() throws IOException {
		 
		 family = "ShipmentRdata";
		 String tableNameaux = '"'+tableName+'"';;
		 String familyaux = '"'+family+'"';
		 
		 OkHttpClient client2 = new OkHttpClient().newBuilder()
				 .build();
		 MediaType mediaType = MediaType.parse("text/xml");
		 RequestBody body2 = RequestBody.create(mediaType, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><TableSchema name="+tableNameaux+"><ColumnSchema name="+familyaux+"/></TableSchema>");
		 Request request2 = new Request.Builder()
				 .url("http://who-dev.essi.upc.edu:60000/"+tableName+"/schema")
				 .method("POST", body2)
				 .addHeader("Accept", "text/xml")
				 .addHeader("Content-Type", "text/xml")
				 .build();
		 Response response2 = client2.newCall(request2).execute();
		 Integer statusCode2 = response2.code();
		 String msg2 = response2.message();
		 
	 }

	private void setProperties(String ctrlPath) {
		String sTableName, sFamily1;
		try {

			////////////////////
			Properties prop = new Properties();

			try {
				InputStream input = new FileInputStream(ctrlPath);
				// load a properties file
				prop.load(input);
				// get the property value and print it out
				sTableName = prop.getProperty("datatableName");
				sFamily1 = prop.getProperty("family1");
				//////////////////
				setTableName(sTableName);
				setFamily(sFamily1);
			} catch (IOException ex) {
				ex.printStackTrace();
			}


		} catch ( Exception e){
			e.printStackTrace();
		}
	}
	
	public void setTableName(String str){this.tableName = str;}
	public void setFamily(String str){this.family = str;}
	
	public String getTableName() {return this.tableName;}
	public String getFamily() {return this.family;}

}
