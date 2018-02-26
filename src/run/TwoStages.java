package run;

import connection.DatabaseProperty;
import connection.DatabasePropertyList;
import extraction.Journal;
import extraction.Predictor;
import meta.ForeignConstraint;
import meta.OutputTable;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class TwoStages {

	// Note: The only thing that has any effect is tableSet. columnSet and patternSet do not affect propagation.
	// Predictor setting is read directly from XML.
	public static void setExploitationPhase(Setting setting, String databasePropertyName, Journal journal) {
		List<Predictor> predictorList =  journal.getAllTopPredictors();
		Set<String> tableSet = new TreeSet<>();
		Set<String> columnSet = new TreeSet<>();
		Set<String> patternSet = new TreeSet<>();


		for (Predictor predictor : predictorList) {
			// Get list of useful tables (including tables in the propagation path)
			tableSet.add(predictor.getOriginalTable());
			tableSet.addAll(predictor.getPropagationPath());

			// Get list of useful columns (in table.column format)
			for (String column : predictor.getColumnMap().values()) {
				columnSet.add(predictor.getOriginalTable() + "." + column);
			}
			// Include the relationship columns from the table and the father table
			for (ForeignConstraint fc : predictor.getTable().foreignConstraintList) {
				for (String column : fc.column) {
					columnSet.add(fc.table + "." + column);
				}
				for (String fColumn : fc.fColumn) {
					columnSet.add(fc.fTable + "." + fColumn);
				}
			}
			// Include the relationship columns from the propagation path
			for (OutputTable propagationTable : predictor.getTable().propagationTables) { // Iterate over all tables in the path...
				ForeignConstraint fc = propagationTable.propagationForeignConstraint; // ...select the FK used in the propagation...
				if (fc!=null) { // For base_sampled it is null as it is the root of the tree
					// ...and iterate over all columns in the relationship
					for (String column : fc.column) {
						columnSet.add(fc.table + "." + column);
					}
					for (String fColumn : fc.fColumn) {
						columnSet.add(fc.fTable + "." + fColumn);
					}
				}
			}

			// Get list of useful patterns
			patternSet.add(predictor.getPatternName());
		}

		// Exclude generated base_sampled
		tableSet.remove(setting.baseSampled);
		columnSet.removeIf(it -> it.startsWith(setting.baseSampled + "."));

		// Include special columns
		for (String targetColumn : setting.targetColumnList) {
			columnSet.add(setting.targetTable + "." + targetColumn);
		}
		for (String targetId : setting.targetIdList) {
			columnSet.add(setting.targetTable + "." + targetId);
		}
		if (setting.targetDate != null) {
			columnSet.add(setting.targetTable + "." + setting.targetDate);
		}

		// Modify the current databaseProperty to use only the useful {tables, columns, patterns}
		DatabasePropertyList propertyList = DatabasePropertyList.unmarshall();
		DatabaseProperty original = propertyList.getDatabaseProperties(databasePropertyName);

		DatabaseProperty copy = (DatabaseProperty) original.clone();
		copy.whiteListColumn = String.join(",", columnSet);
		copy.whiteListTable = String.join(",", tableSet);
		copy.whiteListPattern = String.join(",", patternSet);
		copy.blackListColumn = null;
		copy.blackListTable = null;
		copy.blackListPattern = null;
		copy.sampleCount = null;
		copy.isExploitationPhase = true;
		copy.name = "exploitationStage";    // We have to give a name to the record

		propertyList.setDatabaseProperties(copy);
		DatabasePropertyList.marshall(propertyList);
	}
}
