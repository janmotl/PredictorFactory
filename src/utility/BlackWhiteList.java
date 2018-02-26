package utility;


import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

@SuppressWarnings("unchecked")
public class BlackWhiteList {
	// Logging
	private static final Logger logger = Logger.getLogger(BlackWhiteList.class.getName());

	// Apply black/white lists
	// The map should contain a map of all items.
	// The behaviour was checked against SAS and is in agreement if keep= parameter precedes drop= parameter (this is
	// because parameters in SAS are read from left to right and only once all the parameters are read, the result is
	// applied). Hence, if an item is in both, blacklist and whitelist, blacklist has the preference.
	// The default behaviour is to return all.
	public static SortedMap filter(SortedMap map, Collection blackList, Collection whiteList) {

		// If whitelist/blacklist contains an item not present in the map, log it.
		if (blackList != null) {
			for (Object o : blackList) {
				if (!map.keySet().contains(o))
					logger.warn("Item '" + o + "' from the blacklist was not found in the map. The map contains only: " + map.keySet());
			}
		}
		if (whiteList != null) {
			for (Object o : whiteList) {
				if (!map.keySet().contains(o))
					logger.warn("Item '" + o + "' from the whitelist was not found in the map. The map contains only: " + map.keySet());
			}
		}

		// Do not modify the original map
		SortedMap result = new TreeMap(map);

		if ((whiteList != null) && !whiteList.isEmpty()) { // If the list is empty, ignore the list
			result.keySet().retainAll(whiteList);  // If whiteList is used, perform intersect with the available keys
		}
		if (blackList != null) {
			result.keySet().removeAll(blackList); // Remove blackListed keys
		}

		return result;
	}

}
