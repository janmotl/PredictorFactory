package utility;

import org.junit.Assert;
import org.junit.Test;

public class SortTest {

	@Test
	public void integers() {
		Integer[] data = {3, 2, 4, 5};

		Integer[] actual = Sort.findIndexInSortedArray(data);

		Integer[] expected =  {1, 0, 2, 3};
		Assert.assertArrayEquals(expected, actual);
	}

	@Test
	public void doubles() {
		Double[] data = {3., 2.1, 4.5, 5.2};

		Integer[] actual = Sort.findIndexInSortedArray(data);

		Integer[] expected =  {1, 0, 2, 3};
		Assert.assertArrayEquals(expected, actual);
	}

	@Test
	public void doublePrimitives() {
		double[] data = {3., 2.1, 4.5, 5.2};

		int[] actual = Sort.findIndexInSortedArray(data);

		int[] expected =  {1, 0, 2, 3};
		Assert.assertArrayEquals(expected, actual);
	}
}
