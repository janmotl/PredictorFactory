package utility;


import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.SortedMap;

public class BlackWhiteList {

	// Apply black/white lists
	// The map should contain a map of all items.
	// The behaviour was checked against SAS and is in agreement if keep= parameter precedes drop= parameter (this is
	// because parameters in SAS are read from left to right and only once all the parameters are read, the result is
	// applied). Hence, if an item is in both, blacklist and whitelist, blacklist has the preference.
	// The default behaviour is to return all.
	@NotNull public static SortedMap filter(@NotNull SortedMap map, @Nullable Collection blackList, @Nullable Collection whiteList) {

		if ((whiteList != null) && !whiteList.isEmpty()) { // If the list is empty, ignore the list
			map.keySet().retainAll(whiteList);  // If whiteList is used, perform intersect with the available keys
		}
		if (blackList != null) {
			map.keySet().removeAll(blackList); // Remove blackListed keys
		}

		return map;
	}

}
