package utility;


import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

public class NaturalOrderComparatorTest {

	@Test
	public void naturalOrdering() {
		String[] strings = {"muta_44", "muta_188", "muta_32"};

		Arrays.sort(strings, new NaturalOrderComparator());

		Assert.assertArrayEquals(new String[] {"muta_32", "muta_44", "muta_188"}, strings);
    }

	@Test
	public void accentCasePriority() {
		String[] strings = {"Äbc", "äbc", "Àbc", "àbc", "Abc", "abc", "ABC"};

		Arrays.sort(strings, new NaturalOrderComparator());

		// First by basic, second by accent, third by case
		Assert.assertArrayEquals(new String[] {"abc", "Abc", "ABC", "àbc", "Àbc", "äbc", "Äbc"}, strings);
	}

	@Test
	public void singleSpace() {
		String[] strings = {"m 69", "m 07", "m 6"};

		Arrays.sort(strings, new NaturalOrderComparator());

		// Unfortunately, this implementation does not correctly sort {"m 69", "m     07", "m 6"}
		Assert.assertArrayEquals(new String[] {"m 6", "m 07", "m 69"}, strings);
	}

	@Test
	public void suffix() {
		String[] strings = {"m13", "m12", "m12 a", "m12 b", "m11"};

		Arrays.sort(strings, new NaturalOrderComparator());

		Assert.assertArrayEquals(new String[] {"m11", "m12", "m12 a", "m12 b", "m13"}, strings);
	}


	@Test
	public void notEqualTest() {
		SortedSet<String> set = new TreeSet<>(new NaturalOrderComparator());
		set.add("muta_44");
		set.add("MUTA_44"); // We want to differentiate between these two cases because of case sensitive databases

		Assert.assertEquals(2, set.size());
	}



}
