package samples;

import java.io.Reader;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import scala.collection.immutable.Map;
import scala.util.parsing.json.JSONObject;

public class Pack  extends Offset{

	public static String toJson(Object message) {
		// TODO Auto-generated method stub
		
		return new Gson().toJson(message) ;
	}

	public static void pack(Object message) {
		// TODO Auto-generated method stub
		new Gson().toJson(message) ;
 	}
}
