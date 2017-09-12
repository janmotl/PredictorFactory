package utility;


import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

// Passed Zester test
public class BlackWhiteListTest {

	@Test
	public void modification() {
		SortedMap<String, String> map = new TreeMap<>();
		map.put("A","a");
		map.put("B","b");
		map.put("C","c");
		map.put("D","d");
		map.put("E","e");
		List<String> whiteList = new ArrayList<>();
		whiteList.add("A");
		whiteList.add("B");
		whiteList.add("C");
		List<String> blackList = new ArrayList<>();
		blackList.add("C");

		Assert.assertEquals(5, BlackWhiteList.filter(map, null, null).size());
		Assert.assertEquals(2, BlackWhiteList.filter(map, blackList, whiteList).size());
		Assert.assertEquals(4, BlackWhiteList.filter(map, blackList, null).size());
		Assert.assertEquals(3, BlackWhiteList.filter(map, null, whiteList).size());
		Assert.assertEquals(5, map.size()); // Must remain unchanged
    }

}
