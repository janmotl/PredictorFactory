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


	public static void setExploitationPhase(String databasePropertyName, Journal journal) {
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
					columnSet.add(fc.table + "." + column);
				}
				for (String fColumn : fc.fColumn) {
					columnSet.add(fc.fTable + "." + fColumn);
				}
			}

			// Get list of useful patterns
			patternSet.add(predictor.getPatternName());
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
