import org.apache.kafka.common.protocol.types.Field;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.bson.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Twitter_Data_Filter {

    Document filter(String msg) {
        Logger logger= LoggerFactory.getLogger(Twitter_Data_Filter.class.getName());
        Object Obj;
        Document doc = new Document();
        try {
            Obj = new JSONParser().parse(msg);
        } catch (ParseException e) {
            return doc;
        }
        logger.info("after parse");

        JSONObject JO = (JSONObject) Obj;
        JSONObject user = (JSONObject) JO.get("user");
        String location = (String) user.get("location");
        String time = (String) JO.get("created_at");

        if (location == null)
            return doc;

        logger.info("country start");
        // Country Extraction
        String country;
        String loc[] = location.split(",");
        int len = loc.length;
        country = loc[len - 1];
       // country = country.replaceAll("[^a-zA-Z0-9]", "");
        country = country.trim();
        if(country == "")
            return doc;

        logger.info("tweet start");
        // Tweet Extraction
        String tweet;
        JSONObject J1 = (JSONObject) JO.get("retweeted_status");
        if (J1 == null) {
            JSONObject J2 = (JSONObject) JO.get("extended_tweet");
            if (J2 == null) {
                tweet = (String) JO.get("text");
            } else {
                tweet = (String) J2.get("full_text");
            }
        } else {
            JSONObject J3 = (JSONObject) J1.get("extended_tweet");

            if (J3 == null) {

                tweet = (String) J1.get("text");
            } else {
                tweet = (String) J3.get("full_text");
            }
        }
       // tweet = tweet.replaceAll("[^a-zA-Z0-9]", " ");


        logger.info("date start");

        // day and month extraction
        String time_split[] = time.split(" ");
        String day = time_split[2];
        String month = time_split[1];
        String year = time_split[time_split.length-1];

        doc.put("country", country);
        doc.put("location",location);
        doc.put("day", day);
        doc.put("month", month);
        doc.put("tweet", tweet);
        doc.put("Year",year);
        return doc;
    }
}