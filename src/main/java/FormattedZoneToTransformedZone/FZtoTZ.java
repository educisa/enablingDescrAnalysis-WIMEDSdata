package FormattedZoneToTransformedZone;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FZtoTZ {

	
	public FZtoTZ(){}
	String fz_DBurl, fz_DBusr, fz_DBpwd;//formatted zone, where the schema is
	String FZdbname, FZhost, FZport, FZupdatable; //useful for creating the foreign connection
	String tz_DBurl, tz_DBusr, tz_DBpwd;//transformed zone, where MV are
	//es una nova transformed zone de prova, li diré provaTZdb
	
	

	public String getSQLst() {
		
		//generar sql statements for having foreign tables inside transformed zone and can generate MV from them
		String SQLst = "create extension if not exists postgres_fdw;"; //create 
		//set up the connection to the foreign database, that is, formatted zone
		String stConnForeignDB = "create server formattedZone_foreign_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS ("
				+"dbname '"+FZdbname+"',"
				+"host '"+FZhost+"',"
				+"port '"+FZport+"',"
				+"updatable '"+FZupdatable+"');";
		
		//set up user mapping
		String setupUSRmapping = "CREATE USER MAPPING FOR public SERVER formattedZone_foreign_server OPTIONS("
		+"user '"+fz_DBusr+"',"
		+"password '"+tz_DBpwd+"');";
		
		//create schema statement
		String createschema="CREATE SCHEMA FZ_foreign_tables;";

		//it is strongly reccomended to put the foreign tables in their own schema for tidiness- since Pg does cross-schema queries , no problem there
		//we will have the foreign tables in their own little area so people do not get confused when looking at them
		
		//import foreign schema, for having foreign tables int transformed zone
		//should be parametrized
		String importsFormattedZoneSchema="IMPORT FOREIGN SCHEMA public "
		//--LIMIT to the tables strictly needed, i need all of them since I already have the needed ones in formatted zone db
		+"FROM server formattedZone_foreign_server "
		+"INTO FZ_foreign_tables;";
		
		//with these statements we already have formatted zone tables as foreign tables inside our transformed zone database
		//send sql string to the db driver
		
		
		//I join the 5 steps in one string and send it through the JDBC Driver
		String statement = SQLst+stConnForeignDB+setupUSRmapping+createschema+importsFormattedZoneSchema;
		return statement;
	}
	
	
	
	public void LoadInDB(String SQLstatement, String tz_DBurl, String tz_DBLogin, String tz_DBPassword) {
		Connection cn = null;
		Statement st = null;	
		System.out.println("...working on statements...");
		try {
			//Step 1 : loading driver
			Class.forName("org.postgresql.Driver");
			//Step 2 : Connection
			cn = DriverManager.getConnection(tz_DBurl, tz_DBLogin, tz_DBPassword);
			//Step 3 : creation statement
			st = cn.createStatement();
			
			// Step 5 execution SQLQuery
			st.executeUpdate(SQLstatement);
			System.out.println("statament executed succesfully!");
			
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
	
	
	
	
	public void setProperties(String ctrlPath) throws IOException {
		Properties props = new Properties();
        FileInputStream in = new FileInputStream(ctrlPath);
        try {
			props.load(in);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		this.setDBurl(props.getProperty("fz_DBurl"));
		this.setDBusr(props.getProperty("fz_DBusr"));
		this.setDBpwd(props.getProperty("fz_DBpwd"));
		
		this.setFZdbname(props.getProperty("FZdbname"));
		this.setFZhost(props.getProperty("FZhost"));
		this.setFZport(props.getProperty("FZport"));
		this.setFZupdatable(props.getProperty("FZupdatable"));
		
		this.setTZDBurl(props.getProperty("provaTZdbURL"));
		this.setTZDBusr(props.getProperty("provaTZdbUSR"));
		this.setTZDBpwd(props.getProperty("provaTZdbPWD"));
		
        try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		/////////
		
	}
	
	//getters and setters
	public void setDBurl(String str)     {this.fz_DBurl = str;}
	public String getDBurl()             {return fz_DBurl;}
	public void setDBusr(String str)     {this.fz_DBusr = str;}
	public String getDBusr()             {return fz_DBusr;}
	public void setDBpwd(String str)     {this.fz_DBpwd = str;}
	public String getDBpwd()             {return fz_DBpwd;}
	
	public void setFZdbname(String str)     {this.FZdbname = str;}
	public String getFZdbname()             {return FZdbname;}
	public void setFZhost(String str)     {this.FZhost = str;}
	public String getFZhost()             {return FZhost;}
	public void setFZport(String str)     {this.FZport= str;}
	public String getFZport()             {return FZport;}
	public void setFZupdatable(String str) {this.FZupdatable=str;}
	public String getFZupdatable()		{return this.FZupdatable;}

	
	//transformed zone
	public void setTZDBurl(String str)     {this.tz_DBurl = str;}
	public String getTZDBurl()             {return tz_DBurl;}
	public void setTZDBusr(String str)     {this.tz_DBusr = str;}
	public String getTZDBusr()             {return tz_DBusr;}
	public void setTZDBpwd(String str)     {this.tz_DBpwd = str;}
	public String getTZDBpwd()             {return tz_DBpwd;}
}
