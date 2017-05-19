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
	
	public static <T, U extends Number> T getMaxCategory(HashMap<T, U> values){
		T maxCategory = null;
		Double maxValue = 0.0;
		for(T category : values.keySet()){
			U value = values.get(category);
			if(Math.abs(value.doubleValue()) > Math.abs(maxValue.doubleValue())){
				maxCategory = category;
				maxValue = value.doubleValue();
			}
		}
		return maxCategory;
	}
}
