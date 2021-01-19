package WIMEDSbonitaToHBase;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class getAdminUnitMultiThreading {
	
	public getAdminUnitMultiThreading() {}
	
	public static void main(String[] args) throws IOException, Exception{
		System.out.println("==Application started==");
		//primer agafem el content en forma de JSONArray amb la crida a la API
		JSONArray content = getAdminUnitMultiThreading.getContentArray();
		//després el partim a trossos i cada thread crea la seva partialQuery
		int THREADS =  Runtime.getRuntime().availableProcessors();
		System.out.println("using "+ THREADS+" threads");
		
		//call ParallelPartialQueryGenerator
		ParallelPartialQueryGenerator partialQueryGen = new ParallelPartialQueryGenerator(THREADS);
		String query = partialQueryGen.sum(content);
		
		//load to postgresDB
		String url = "jdbc:postgresql://localhost:5432/AdminUnitDB";
		String user = "postgres";
		String password = "postgres";

		getAdminUnitMultiThreading.LoadInDB(query, url, user, password);
	}
	
	public static JSONArray getContentArray() throws IOException {

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/bdm/businessData/com.company.model.AdministrationUnit?q=find&p=0&c=106307")
				.method("GET", null)
				.addHeader("Cookie", "bonita.tenant=1; JSESSIONID=422BD90BF1164956A49B2230C09A5B85; X-Bonita-API-Token=96c82428-876a-45a4-9a93-a0bb90b3190f; BOS_Locale=en")
				.build();
		Response response = client.newCall(request).execute();
		
		ResponseBody body = response.body();

		String bodyString = response.body().string();

		JSONArray resultJSONArray = new JSONArray(bodyString);
		System.out.println(resultJSONArray.length());
		
		return resultJSONArray;//return 
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
