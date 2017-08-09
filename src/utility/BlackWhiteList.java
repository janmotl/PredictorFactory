package utility;


import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

@SuppressWarnings("unchecked")
public class BlackWhiteList {

	// Apply black/white lists
	// The map should contain a map of all items.
	// The behaviour was checked against SAS and is in agreement if keep= parameter precedes drop= parameter (this is
	// because parameters in SAS are read from left to right and only once all the parameters are read, the result is
	// applied). Hence, if an item is in both, blacklist and whitelist, blacklist has the preference.
	// The default behaviour is to return all.
	public static SortedMap filter(SortedMap map, Collection blackList, Collection whiteList) {

		SortedMap result = new TreeMap(map); // Do not modify the original map

		if ((whiteList != null) && !whiteList.isEmpty()) { // If the list is empty, ignore the list
			result.keySet().retainAll(whiteList);  // If whiteList is used, perform intersect with the available keys
		}
		if (blackList != null) {
			result.keySet().removeAll(blackList); // Remove blackListed keys
		}

		return result;
	}

}
