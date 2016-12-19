package utility;


import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

public class NaturalOrderComparatorTest {

	@Test
	public void orderingTest() {
		String[] strings = new String[] {"muta_44", "muta_188", "muta_32"};

		Arrays.sort(strings, new NaturalOrderComparator());

		Assert.assertArrayEquals(new String[]{"muta_32", "muta_44", "muta_188"}, strings);
    }

	@Test
	public void notEqualTest() {
		SortedSet<String> set = new TreeSet<>(new NaturalOrderComparator());
		set.add("muta_44");
		set.add("MUTA_44"); // We want to differentiate between these two cases because of case sensitive databases

		Assert.assertEquals(2, set.size());
	}



}
