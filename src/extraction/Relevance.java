package extraction;

import run.Setting;

import java.util.HashMap;
import java.util.Map;

// Relevance of the predictor for classification.
// We assume that each measure has to be maximized.
//
// Note: I tried to use "Table" with {BaseTarget, Measure, Value}, respectively "HashBasedTable" from Guava as the data
// storage. But I did not manage to get it serializable (No default constructor found on class
// com.google.common.collect.HashBasedTable). It is possible to replace Table<R,C,V> with Map<R,Map<C,V>>.
// But I simply opted for 2 maps.
// FOR INSPIRATION HOW TO DEAL WITH A MIXTURE OF MEASURES WHERE WE MAXIMIZE AND MINIMIZE A VALUE SEE RAPIDMINER
public class Relevance {
	private Map<String, Double> relevance = new HashMap<>();        // BaseTarget -> Chi2/R2
	private Map<String, Double> conceptDrift = new HashMap<>();     // BaseTarget -> 0..1

	// Constructor
	public Relevance() {

	}

	// Copy constructor
	public Relevance(Relevance other) {
		relevance = new HashMap<>(other.relevance);         // Shallow copies
		conceptDrift = new HashMap<>(other.conceptDrift);
	}


	// We multiply Chi2 and conceptDrift.
	// Note: A geometric average could be a reasonable generalization to multiple measures as the individual measures
	// do not have to be commensurable (have the same scale, distribution,...).
	public double getWeightedRelevance(String baseTarget) {
		return getRelevance(baseTarget) * getConceptDrift(baseTarget);
	}

	public double getRelevance(String baseTarget) {
		Double result = relevance.get(baseTarget);
		if (result==null) return 0.0;     // Default value
		return result;
	}

	public double getConceptDrift(String baseTarget) {
		Double result = conceptDrift.get(baseTarget);
		if (result==null) return 1.0;     // Default value
		return result;
	}

	public void setRelevance(String baseTarget, double value) {
		relevance.put(baseTarget, value);
	}

	public void setConceptDrift(String baseTarget, double value) {
		conceptDrift.put(baseTarget, value);
	}

	@Override
	public String toString() {
		return "conceptDrift=" + conceptDrift + ", relevance=" + relevance;
	}

	// Calculate Chi2 and conceptDrift for each target
	// Note: does it have to be static? The usage in Aggregation is cumbersome...
	public static Relevance calculate(Setting setting, Predictor predictor) {
		Relevance relevance = new Relevance();

		if (predictor.getBaseTarget()==null) {
			// Looks like an aggregate -> calculate weighted relevance for each target individually
			for (String baseTarget : setting.baseTargetList) {
				relevance.setRelevance(baseTarget, calculateRelevance(setting, predictor, baseTarget));
				relevance.setConceptDrift(baseTarget, calculateConceptDrift(setting, predictor, baseTarget));
			}
		} else {
			// Looks like WoE -> calculate weighted relevance only for that single target, leave the rest of the targets at the default value
			relevance.setRelevance(predictor.getBaseTarget(), calculateRelevance(setting, predictor, predictor.getBaseTarget()));
			relevance.setConceptDrift(predictor.getBaseTarget(), calculateConceptDrift(setting, predictor, predictor.getBaseTarget()));
		}

		return relevance;
	}

	// Calculate concept drift (so far just for nominal labels & if targetDate exists)
	private static double calculateConceptDrift(Setting setting, Predictor predictor, String baseTarget) {
		if ("classification".equalsIgnoreCase(setting.task) && setting.targetDate!=null) {
			return setting.dialect.getConceptDrift(setting, predictor, baseTarget);
		} else {
			return 1.0; // If concept drift is not applicable, return a value that is invariant
		}
	}

	// Calculate univariate relevance estimate
	private static double calculateRelevance(Setting setting, Predictor predictor, String baseTarget) {
		if ("classification".equalsIgnoreCase(setting.task)) {
			return setting.dialect.getChi2(setting, predictor, baseTarget);
		} else {
			return setting.dialect.getR2(setting, predictor, baseTarget);
		}
	}


}
