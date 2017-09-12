package utility;

public class NullSafeComparator {
	// See: http://stackoverflow.com/questions/481813/how-to-simplify-a-null-safe-compareto-implementation
	public static int nullSafeStringComparator(final String one, final String two) {
		if (one == null ^ two == null) {	// XOR
			return (one == null) ? -1 : 1;  // Null is first
		}

		if (one == null && two == null) { // The second condition is always true but for clarity it is left there
			return 0;
		}

		return one.compareTo(two);
	}
}
