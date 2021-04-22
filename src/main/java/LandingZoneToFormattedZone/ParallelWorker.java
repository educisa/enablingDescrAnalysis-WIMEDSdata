package LandingZoneToFormattedZone;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.json.JSONArray;
import org.json.JSONObject;

public class ParallelWorker extends Thread{

	private JSONArray content; //JSONArray with all the AdminUnit or Request from the REST API response
	private int low, high;
	private String partialQuery; //partial query inserting AdminUnit in range low,high in AdminUnit JSONArray
	private String whichData;


	public ParallelWorker(JSONArray content, int low, int high, String whichData) {
		this.content = content;
		this.low = low;
		this.high = high;
		this.whichData = whichData;
	}


	@Override
	public void run() {
		partialQuery = "";

		if(this.whichData.equals("AdministrationUnit")) {

			for(int i=low; i<high; ++i) {
				//generate query for each adminunit between the range low,high
				JSONObject AdminUnitObj = content.getJSONObject(i);
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


				partialQuery += "INSERT INTO AdministrationUnit VALUES ('"
						+ id + "','"
						+ parentid + "','"
						+ name.replaceAll("'","''") + "','"
						+ shortname.toString().replaceAll("'","''") + "','"
						+ datelastupdated.substring(0,10) + "',"
						+ leaf + ","
						+ levelnumber + ",'"
						+ address + "','"
						+ url + "') ON CONFLICT ON CONSTRAINT administrationunit_pkey"
						+ " DO"
						+ "  UPDATE"
						+ "  SET   "
						+ "parentid='"
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
						+" WHERE AdministrationUnit.id='"+id+"';\n";

			}
		}

		if(this.whichData.equals("Request")) {
			for(int i = low;i<high; ++i) {
				int id, diseaseID, medicalSupplyID, requestStatus, weightInKg, age, quantity;
				String countryID, healthFacilityID, requestDateString, healthFacility, phase, transmissionWay;

				JSONObject requestJSONObj = content.getJSONObject(i);
				id = requestJSONObj.getInt("persistenceId");
				countryID = requestJSONObj.getJSONObject("countryAdminUnit").getString("id");
				diseaseID = requestJSONObj.getJSONObject("disease").getInt("persistenceId");
				medicalSupplyID = requestJSONObj.getJSONObject("medicalSupply").getInt("persistenceId");
				requestStatus = requestJSONObj.getJSONObject("requestStatus").getInt("persistenceId");
				//healthFacilityID = requestJSONObj.getString("healthFacilityPid");
				requestDateString = requestJSONObj.getString("requestDate");
				requestDateString = requestDateString.substring(0,10);
				healthFacility = requestJSONObj.getString("currentHealthFacilityName");
				weightInKg = requestJSONObj.getInt("weightInKg");
				age = requestJSONObj.getInt("age");
				phase = requestJSONObj.getString("phase");
				transmissionWay = requestJSONObj.getString("transmissionWay");
				quantity = requestJSONObj.getInt("quantity");

				partialQuery += "INSERT INTO request VALUES ("
						+ id + ",'"
						+ countryID + "','"
						+ healthFacility + "',"
						+ diseaseID + ","
						+ medicalSupplyID + ","
						+ requestStatus + ",'"
						+ requestDateString + "',"
						+ age + ","
						+ weightInKg + ",'"
						+ phase +"','"
						+ transmissionWay +"',"
						+ quantity +") ON CONFLICT ON CONSTRAINT request_pkey"
						+ " DO"
						+ " UPDATE"
						+ " SET"
						+" countryID='"+countryID+"',"
						+" healthFacility='"+healthFacility+"',"
						+" diseaseID="+diseaseID+","
						+" medicalSupplyID="+medicalSupplyID+","
						+" requestStatusid="+requestStatus+","
						+" requestDate='"+requestDateString+"',"
						+" patientage="+age+","
						+" patientweightinkg="+weightInKg+","
						+" diseasephase='"+phase+"',"
						+" transmissionway='"+transmissionWay+"',"
						+" quantity="+quantity+";\n";

			}
		}

		if(this.whichData.equals("ShipmentR")) {
			for(int i = low;i<high; ++i) {

				int id, medicalsupplyid,quantity, requestID, trackingNumber;
				Integer quantityReceived = null;//int can not be null
				String receptionDateString = null;
				String shippedDateString = null;
				String courierName = null;
				String shipmentStatus, shipmentDateCreationString, EDDstring, healthFacilityName;

				JSONObject shipmentJSONObj = content.getJSONObject(i);
				id = shipmentJSONObj.getInt("persistenceId");
				quantity = shipmentJSONObj.getInt("quantity");
				healthFacilityName = shipmentJSONObj.getString("healthFacilityName");
				Object aObj = shipmentJSONObj.get("quantityReceived");
				if (aObj instanceof Integer) {
					quantityReceived = shipmentJSONObj.getInt("quantityReceived");//if not received is null
				}
				aObj = shipmentJSONObj.get("receptionDate"); //if not received is null
				if (aObj instanceof String) {
					receptionDateString = shipmentJSONObj.getString("receptionDate");
				}
				medicalsupplyid = shipmentJSONObj.getJSONObject("medicalSupply").getInt("persistenceId");
				requestID = shipmentJSONObj.getJSONObject("request").getInt("persistenceId");
				shipmentDateCreationString = shipmentJSONObj.getString("dateOfCreation");
				shipmentDateCreationString = shipmentDateCreationString.substring(0,10);
				EDDstring = shipmentJSONObj.getString("edd");
				aObj = shipmentJSONObj.get("shippedDate");// if not shipped is null
				if (aObj instanceof String) {
					shippedDateString = shipmentJSONObj.getString("shippedDate");
				}
				shippedDateString = shipmentJSONObj.getString("shippedDate");
				shipmentStatus = shipmentJSONObj.getString("shipmentStatus");
				courierName = shipmentJSONObj.getJSONObject("courier").getString("name");
				trackingNumber = shipmentJSONObj.getInt("trackingNumber");

				partialQuery += "INSERT INTO shipmentR VALUES ("
						+ id + ",'"
						+ shipmentDateCreationString + "','"
						+ shipmentStatus + "','"
						+ EDDstring + "','";
				if(shippedDateString==null)partialQuery+=null+",";
				else partialQuery+=shippedDateString+"',";
				if(receptionDateString == null)partialQuery+=null + ",";
				else partialQuery += "'"+receptionDateString + "',";
				partialQuery += requestID + ","
						+ medicalsupplyid + ",'"
						+ healthFacilityName + "',"
						+ quantity + ",";
				if(quantityReceived == null)partialQuery+=null + ",";
				else partialQuery += quantityReceived+",";
				if(courierName == null)partialQuery+=null + ",";
				else partialQuery += "'"+courierName+"',";
				partialQuery+= trackingNumber+") ON CONFLICT ON CONSTRAINT shipmentr_pkey"
						+ " DO UPDATE SET"
						+ " shipmentcreationdate='"+shipmentDateCreationString+"',"
						+ " shipmentstatus='"+shipmentStatus+"',"
						+ " EDD='"+EDDstring+"',";
				if(shippedDateString==null)partialQuery+= " shippedDate="+shippedDateString+",";
				else partialQuery+= " shippedDate='"+shippedDateString+"',";
				if(receptionDateString==null)partialQuery+= " receptionDate="+receptionDateString+",";
				else partialQuery+= " receptionDate='"+receptionDateString+"',";	
				partialQuery+= " medicalsupplyid="+medicalsupplyid+","
						+ " healthFacilityName='"+healthFacilityName+"',"
						+ " quantity="+quantity+","
						+ " quantityreceived="+quantityReceived+",";
				if(courierName==null)partialQuery+= " courierName="+courierName+",";
				else partialQuery+= " courierName='"+courierName+"',";
				partialQuery+=" trackingNumber="+trackingNumber+";\n";
			}
		}
	}

	public String getPartialQuery() {
		return this.partialQuery;
	}

}
