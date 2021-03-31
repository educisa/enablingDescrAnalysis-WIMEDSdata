package WIMEDSbonitaToHBase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Base64;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class exampleGetAdminUnit {
	
	public exampleGetAdminUnit() {}
	
	public static void main(String[] args) throws IOException, Exception{
		
		//primer agafem el content en forma de JSONArray amb la crida a la API
		//després el partim a trossos i cada thread crea la seva partialQuery
		//juntem les partialsQuerys i fem LoadInDB
		String query = exampleGetAdminUnit.getAdminUnit();
		//load to postgresDB
		String url = "jdbc:postgresql://localhost:5432/AdminUnitDB";
		String user = "postgres";
		String password = "postgres";
		
		exampleGetAdminUnit.LoadInDB(query, url, user, password);
	}
	
	public static String getAdminUnit() throws IOException {

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/bdm/businessData/com.company.model.AdministrationUnit?q=find&p=0&c=106307")
				.method("GET", null)
				.addHeader("Cookie", "bonita.tenant=1; JSESSIONID=5967FC993361512E87822FD5C815B871; X-Bonita-API-Token=f1c79280-cd48-4048-964d-ebbf9406e315; BOS_Locale=en")
				.build();
		Response response = client.newCall(request).execute();
		
		ResponseBody body = response.body();

		String bodyString = response.body().string();

		JSONArray resultJSONArray = new JSONArray(bodyString);
		System.out.println(resultJSONArray.length());
		
		String SQLQuery="";
		int extractedRows = 0;
		for(int i = 0; i < resultJSONArray.length(); ++i) {
			JSONObject rowJSONObj = (JSONObject) resultJSONArray.get(i);

			++extractedRows;
			if(extractedRows%30000 == 0)System.out.println("extractedRows since started:"+ extractedRows);
			if(i == 0) {
				System.out.println(rowJSONObj);
			}
			
			String parentid = "";
			if(rowJSONObj.has("parent")) {
				JSONObject JSONObjparent = rowJSONObj.getJSONObject("parent");
				parentid = JSONObjparent.getString("id");
			}
			
			String url = null;//just some organisationUnits have (e.g., http://www.hospitalclinic.org)
			if(rowJSONObj.has("url")) {
				url = rowJSONObj.getString("url");
				url = url.replaceAll("'","''");
			}
			
			String address = null;//just some organisationUnits have (e.g., calle Villarroel, 170, 08036 Barcelona, Spain)
			if(rowJSONObj.has("address")) {
				address = rowJSONObj.getString("address");
				address = address.replaceAll("'","''");
			}
			
			String id, name, shortname, datelastupdated;
			Boolean leaf;
			Integer levelnumber;
			id = rowJSONObj.getString("id");
			name = rowJSONObj.getString("name");
			shortname = rowJSONObj.getString("shortName");
			datelastupdated = rowJSONObj.getString("dateLastUpdated");
			leaf = rowJSONObj.getBoolean("leaf");
			levelnumber = rowJSONObj.getInt("levelNumber");
			
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
					
					
		}System.out.println(extractedRows);
		return SQLQuery;
	}
	
	
	public static void LoadInDB(String SQLQuery, String DBurl, String DBLogin, String DBPassword) {
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
	
	
	
	
	
}
