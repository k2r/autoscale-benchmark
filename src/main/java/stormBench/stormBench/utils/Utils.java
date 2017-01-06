/**
 * 
 */
package stormBench.stormBench.utils;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Roland
 *
 */
public class Utils {

	public static <T,U> ArrayList<String> convertToInfo(HashMap<T, U> map){
		ArrayList<String> results = new ArrayList<>();
		for(T key : map.keySet()){
			U value = map.get(key);
			String info = key + " ---> " + value;
			results.add(info);
		}
		return results;
	}
}
