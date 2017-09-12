package mother;

import extraction.Pattern;

public abstract class PatternMother {

	public static Pattern woe() {
		Pattern woe = new Pattern();

		woe.dialectCode = "select 3";
		woe.name = "WoE";
		woe.author = "Thorough Tester";
		woe.cardinality = "n";
		woe.description = "Once upon a time...";

		return woe;
	}

	public static Pattern aggregate() {
		Pattern woe = new Pattern();

		woe.dialectCode = "select 3";
		woe.name = "Aggregate";
		woe.author = "Thorough Tester";
		woe.cardinality = "n";
		woe.description = "Once upon a time...";

		return woe;
	}
}
