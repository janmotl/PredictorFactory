package propagation;

import meta.Column;
import meta.MetaOutput;
import meta.StatisticalType;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import run.Setting;

import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;

public class TemporalConstraint {

	// Logging
	private static final Logger logger = Logger.getLogger(TemporalConstraint.class.getName());

	// Sets the most likely time constraint. If no time constraint is found, table.constraintDate remains null.
	// The function sets: {constraintDate, temporalConstraintJustification, temporalConstraintRowCountOptimistic, isIdUnique}.
	@NotNull public static MetaOutput.OutputTable find(@NotNull Setting setting, @NotNull MetaOutput.OutputTable table) {

		// 1) If setting.targetDate is not defined, stop. Leave null in the table.constraintDate.
		if (setting.targetDate == null) {
			table.temporalConstraintJustification = "No targetDate was defined. If this is not a static dataset, check the initial setting of Predictor Factory.";
			return table;
		}

		// 2) If there isn't any candidate, stop. Unfortunately, dates stored as a String or a number are ignored.
		if (table.getColumns(setting, StatisticalType.TEMPORAL).isEmpty()) {
			table.temporalConstraintJustification = "No candidate for a time constrain. Are the temporal attributes stored as timestamp/datetime/date or time?";
			return table;
		}

		// 3) If idColumn is distinct in the table, it is unnecessary to set time condition.
		// For example, in Customer table, which contains only static information, like Birth_date,
		// it is not necessary to set the time constrain.
		// NOTE: if the id is a primary key, I know the relationship between the target table and the non-target
		// table must be 1:1 or n:1. Either way, the id is unique. This may accelerate the query, if the planer is dumb.
		table.isIdUnique = setting.dialect.isIdUnique(setting, table);
		if (table.isIdUnique) {
			table.temporalConstraintJustification = "Static table. The relationship between targetTable and this table appears to be in 1:1 or n:1. This is a sign of a static table or a table without old versions of the data.";
			return table;
		}

		// 4) Get columns without nulls (there isn't any good reason why a timestamp should be missing).
		// NOTE: if there is a not null constraint, automatically could trust it (unless it is Netezza...)
		SortedSet<Column> candidateSet = table.getColumns(setting, StatisticalType.TEMPORAL);
		candidateSet.removeIf(column -> column.containsNull(setting, table.originalName));

		if (candidateSet.isEmpty()) {
			table.temporalConstraintJustification = "All candidates contain null values. An attribute with the date of entering the data should never be empty.";
			return table;
		}

		// 5) Get columns without any date in the future (relative to database's current time)
		candidateSet.removeIf(column -> column.containsFutureDate(setting, table.originalName));

		if (candidateSet.isEmpty()) {
			table.temporalConstraintJustification = "All candidates either contain null values or dates in the future. An attribute with a future date can't be a date of record entry.";
			return table;
		}

		// Look at the data type. The preference order is: timestamp (in Oracle, timestamps have higher accuracy than
		// datetimes) > datetime > date.
		// If there is just a single timestamp but multiple datetimes, the timestamp is likely to be the time constraint

		// Look at the precision. Prefer records with non-constant "second" part.
		// Should use ODBC function calls.


		// 6) If exactly one of the candidates is NOT nullable, call it a temporal constraint.
		int nonNullableCount = 0;
		String temporalConstraint = null;

		for (Column column : candidateSet) {
			if (!column.isNullable) {
				nonNullableCount++;
				temporalConstraint = column.name;
			}
		}

		if (nonNullableCount == 1) {
			table.temporalConstraint = temporalConstraint;
			table.temporalConstraintJustification = "A single non-null. This is the only temporal attribute that fulfils the requirements for a temporal constraint and is additionally constrained to be non-null.";
			return table;
		}

		// 7) Get an optimistic estimate of count of tuples that fulfill the time constrain (null values are not a match).
		// If the count of dates that fulfil the time constrain is 0, end here.
		NavigableMap<Integer, String> treeMap = new TreeMap<>();

		for (Column column : candidateSet) {
			int count = setting.dialect.countUsableDates(setting, table.originalName, column.name);
			treeMap.put(count, column.name);
			logger.debug("Column " + table.originalName + "." + column.name + " has " + count + " rows that may satisfy the time constraint.");
		}

		table.temporalConstraintRowCountOptimistic = treeMap.lastKey(); // The highest estimated count of propagated rows

		if (candidateSet.isEmpty() || table.temporalConstraintRowCountOptimistic == 0) {
			table.temporalConstraintJustification = "All candidates either contain null values, future dates or all dates are out of the time range defined in the initial setting of Predictor Factory.";
			return table;
		}

		// Return the column that has the biggest count of records fulfilling the time constraint
		table.temporalConstraint = treeMap.lastEntry().getValue();
		table.temporalConstraintJustification = "Has the highest optimistic estimate of the count of rows that are in the time range defined in the initial setting of Predictor Factory.";
		return table;
	}

}
