package utility;

import org.jetbrains.annotations.NotNull;

public class ParseInteger {

	// Try to parse string to Integer. If the conversion fails, return null.
	public static Integer parseInteger(String string) {
		try {
			return Integer.parseInt(string);
		} catch (NumberFormatException ignored) {
			return null;
		}
	}
}
