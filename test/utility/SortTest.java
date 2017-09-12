package utility;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class SortTest {

	@Test
	public void integers() {
		Integer[] data = {3, 2, 4, 5};

		Integer[] actual = Sort.findIndexInSortedArray(data);

		Integer[] expected = {1, 0, 2, 3};
		assertArrayEquals(expected, actual);
	}

	@Test
	public void doubles() {
		Double[] data = {3., 2.1, 4.5, 5.2};

		Integer[] actual = Sort.findIndexInSortedArray(data);

		Integer[] expected = {1, 0, 2, 3};
		assertArrayEquals(expected, actual);
	}

	@Test
	public void doublePrimitives() {
		double[] data = {3., 2.1, 4.5, 5.2};

		int[] actual = Sort.findIndexInSortedArray(data);

		int[] expected = {1, 0, 2, 3};
		assertArrayEquals(expected, actual);
	}

	@Test
	public void emptyArray() {
		double[] empty = {};
		assertArrayEquals(new int[]{}, Sort.findIndexInSortedArray(empty));
	}

	@Test
	public void isStable() {
		double[] zeros = {0, 0, 0, 0};
		assertArrayEquals(new int[]{0, 1, 2, 3}, Sort.findIndexInSortedArray(zeros));
	}

	@Test
	public void strings() {
		String[] strings = {"cherry", "apple", "banana"};
		assertArrayEquals(new Integer[]{1, 2, 0}, Sort.findIndexInSortedArray(strings));
	}
}
