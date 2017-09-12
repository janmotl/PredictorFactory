package utility;

import java.util.Arrays;
import java.util.stream.IntStream;

// See: https://stackoverflow.com/questions/951848/java-array-sort-quick-way-to-get-a-sorted-list-of-indices-of-an-array
public class Sort {

	// Equivalent of Matlab's sort, which returns indexes as the second return value.
	// The input may not be null.
	public static Integer[] findIndexInSortedArray(Comparable[] data) {
		// Make ascending vector: 0..vector.length
		Integer[] index = new Integer[data.length];
		for (int i = 0; i < data.length; i++) {
			index[i] = i;
		}

		// Sort
		Arrays.sort(index, (a, b) -> (data[a].compareTo(data[b])));

		return index;
	}

	public static int[] findIndexInSortedArray(double[] data) {
		int[] sortedIndices = IntStream.range(0, data.length)
				.boxed().sorted((i, j) -> Double.compare(data[i], data[j]))
				.mapToInt(ele -> ele).toArray();

		return sortedIndices;
	}
}
