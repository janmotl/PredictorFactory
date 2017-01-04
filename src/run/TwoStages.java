package run;

import connection.DatabaseProperty;
import connection.DatabasePropertyList;
import extraction.Journal;
import extraction.Predictor;
import meta.ForeignConstraint;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class TwoStages {

	// Note: The only thing that has any effect is tableSet. columnSet and patternSet do not affect propagation.
	// Predictor setting is read directly from XML.
	public static void setExploitationPhase(Setting setting, String databasePropertyName, Journal journal) {
		List<Predictor> predictorList =  journal.getTopPredictors();
		Set<String> tableSet = new TreeSet<>();
		Set<String> columnSet = new TreeSet<>();
		Set<String> patternSet = new TreeSet<>();


		for (Predictor predictor : predictorList) {
			// Get list of useful tables (including tables in the propagation path)
			tableSet.add(predictor.getOriginalTable());
			tableSet.addAll(predictor.getPropagationPath());

			// Get list of useful columns (in table.column format; including the foreign keys)
			// NOTE: SHOULD INCLUDE PK/FK COLUMNS FROM THE PROPAGATION PATH
			for (String column : predictor.getColumnMap().values()) {
				columnSet.add(predictor.getOriginalTable() + "." + column);
			}
			for (ForeignConstraint fc : predictor.getTable().foreignConstraintList) {
				for (String column : fc.column) {
					if (tableSet.contains(fc.table)) columnSet.add(fc.table + "." + column);
				}
				for (String fColumn : fc.fColumn) {
					if (tableSet.contains(fc.fTable)) columnSet.add(fc.fTable + "." + fColumn);
				}
			}

			// Get list of useful patterns
			patternSet.add(predictor.getPatternName());
		}

		// Exclude generated base_sampled. Include special columns.
		tableSet.remove(setting.baseSampled);
		columnSet.add(setting.targetTable + "." + setting.targetColumn);
		if (setting.targetDate != null) {
			columnSet.add(setting.targetTable + "." + setting.targetDate);
		}
		for (String id : setting.targetIdList) {
			columnSet.add(setting.targetTable + "." + id);
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
		copy.name = "exploitationStage";

		propertyList.setDatabaseProperties(copy);
		DatabasePropertyList.marshall(propertyList);
	}
}
