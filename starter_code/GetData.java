import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;
import java.util.HashMap;



//json.simple 1.1
// import org.json.simple.JSONObject;
// import org.json.simple.JSONArray;

// Alternate implementation of JSON modules.
import org.json.JSONObject;
import org.json.JSONArray;

public class GetData{

    static String prefix = "project2.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding
    // tables in your database

    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;

    // This is the data structure to store all users' information
    // DO NOT change the name
    JSONArray users_info = new JSONArray();		// declare a new JSONArray


    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
	super();
	String dataType = u;
	oracleConnection = c;
	// You will use the following tables in your Java code
	cityTableName = prefix+dataType+"_CITIES";
	userTableName = prefix+dataType+"_USERS";
	friendsTableName = prefix+dataType+"_FRIENDS";
	currentCityTableName = prefix+dataType+"_USER_CURRENT_CITIES";
	hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITIES";
	programTableName = prefix+dataType+"_PROGRAMS";
	educationTableName = prefix+dataType+"_EDUCATION";
	eventTableName = prefix+dataType+"_USER_EVENTS";
	albumTableName = prefix+dataType+"_ALBUMS";
	photoTableName = prefix+dataType+"_PHOTOS";
	tagTableName = prefix+dataType+"_TAGS";
    }




    //implement this function

    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException{

    	JSONArray users_info = new JSONArray();

        HashMap<Long, JSONObject> map = new HashMap<Long, JSONObject>();
        HashMap<Long, JSONArray> map_friends = new HashMap<Long, JSONArray>();
    	//Query the oracle
        try (Statement stmt = oracleConnection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY)) {


            // Select users and cities
            ResultSet rst = stmt.executeQuery(
                    "SELECT u.USER_ID,u.FIRST_NAME,u.LAST_NAME,u.YEAR_OF_BIRTH,u.MONTH_OF_BIRTH,u.DAY_OF_BIRTH, u.GENDER,c1.CITY_NAME, c1.STATE_NAME, c1.COUNTRY_NAME, c2.CITY_NAME, c2.STATE_NAME, c2.COUNTRY_NAME  " +
                            "FROM " + userTableName + " u " +
                            "left join " + currentCityTableName + " cc " +
                            "on u.user_id=cc.user_id " +
                            "left join " + hometownCityTableName + " hc " +
                            "on u.user_id=hc.user_id " +
                            "left join "+ cityTableName+" c1 "+
                            "on cc.CURRENT_CITY_ID=c1.city_id "+
                            "left join "+ cityTableName+" c2 "+
                            "on hc.HOMETOWN_CITY_ID=c2.city_id "
            );

            while (rst.next()) {

                JSONObject curUserInfo=new JSONObject();
                map.put(rst.getLong(1), curUserInfo);
                //Get Data from result
                long UserID=rst.getLong(1);
                String FirstName=rst.getString(2);
                String LastName=rst.getString(3);
                long YOB=Long.valueOf(rst.getString(4));
                long MOB=Long.valueOf(rst.getString(5));
                long DOB=Long.valueOf(rst.getString(6));
                String Gender=rst.getString(7);
                //Create current city JSON object
                JSONObject Current=new JSONObject();
                String CCity=rst.getString(8);
                String CState=rst.getString(9);
                String CCountry=rst.getString(10);
                if (!rst.wasNull()) {
                    Current.put("city",CCity);
                    Current.put("country",CCountry);
                    Current.put("state",CState);
                }
                //Create hometown city JSON object
                JSONObject Hometown=new JSONObject();
                String HCity=rst.getString(11);
                String HState=rst.getString(12);
                String HCountry=rst.getString(13);
                if (!rst.wasNull()) {
                    Hometown.put("city",HCity);
                    Hometown.put("country",HCountry);
                    Hometown.put("state",HState);
                }
                //put all information in JSON object
                curUserInfo.put("current",Current);
                curUserInfo.put("hometown",Hometown);
                curUserInfo.put("gender",Gender);
                curUserInfo.put("user_id",UserID);
                curUserInfo.put("YOB",YOB);
                curUserInfo.put("MOB",MOB);
                curUserInfo.put("DOB",DOB);
                curUserInfo.put("last_name", LastName);
                curUserInfo.put("first_name", FirstName);
            }

            //get friend information
            rst = stmt.executeQuery(
                    "SELECT f.USER1_ID, f.USER2_ID  " +
                            "FROM " + friendsTableName + " f " +
                            "WHERE f.USER1_ID < f.USER2_ID " +
                            "ORDER BY USER1_ID, USER2_ID "
            );

            while(rst.next()){
                long uid1=rst.getLong(1);
                long uid2=rst.getLong(2);
                if (!map_friends.containsKey(uid1)) {
                    map_friends.put(uid1, new JSONArray());
                }
                map_friends.get(uid1).put(uid2);
            }


            rst.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        for (Long uid : map.keySet()) {
            JSONObject o = map.get(uid);
            users_info.put(o);
            if (map_friends.containsKey(uid)) {
                o.put("friends", map_friends.get(uid));
            }
            else {
                o.put("friends", new JSONArray());
            }
        }

		return users_info;
    }

    // This outputs to a file "output.json"
    public void writeJSON(JSONArray users_info) {
	// DO NOT MODIFY this function
	try {
	    FileWriter file = new FileWriter(System.getProperty("user.dir")+"/output.json");
	    file.write(users_info.toString());
	    file.flush();
	    file.close();

	} catch (IOException e) {
	    e.printStackTrace();
	}

    }
}
