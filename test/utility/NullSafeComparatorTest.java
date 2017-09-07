package utility;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NullSafeComparatorTest {
	@Test
	public void nullSafeStringComparator() throws Exception {
		assertEquals(0, NullSafeComparator.nullSafeStringComparator("apple", "apple"));
		assertEquals(0, NullSafeComparator.nullSafeStringComparator(null, null));
		assertEquals(-1, NullSafeComparator.nullSafeStringComparator(null, "apple"));
		assertEquals(1, NullSafeComparator.nullSafeStringComparator("apple", null));
		assertEquals(-1, NullSafeComparator.nullSafeStringComparator("apple", "banana"));
		assertEquals(1, NullSafeComparator.nullSafeStringComparator("banana", "apple"));
	}

}