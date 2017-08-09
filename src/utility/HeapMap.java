package utility;

import extraction.Predictor;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import java.util.*;

// A multi-criteria heap.
// Note: The implementation is not memory friendly as we make copies of large objects.
// Furthermore, the implementation is not generalized to other data types (Predictor is hardcoded).
// And the chosen relevance is stored in the Predictor and not in this object -> that is ugly.
// We should have heaps with Integer indexes and store the Predictors (or any object) in an array/List.
// While passing the object, we would also pass a list/map with the relevances...
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({Predictor.class})
public class HeapMap {
	private final Map<String, HeapWithFixedSize<Predictor>> heapMap = new HashMap<>();
	private int featureLimitCount;  // Defines the limit size of the heaps


	// Constructor
	public HeapMap(Collection<String> targetList, Comparator comparator, int limit) {
		featureLimitCount = limit;

		// We create a map<baseTarget, queue>
		for (String target : targetList) {
			heapMap.put(target, new HeapWithFixedSize<>(limit, comparator));
		}
	}

	// JAXB constructor
	public HeapMap() {

	}

	// Add a single predictor into all heaps.
	// Useful for target agnostic predictors like aggregates.
	// Returns a list of dropped predictors (list because we permit multiple targets and a single predictor may evict
	// predictors from multiple heaps).
	public List<Predictor> addPredictor(Predictor predictor) {
		List<Predictor> evictionList = new ArrayList<>();

		// Put the predictor into each queue
		for (String target : heapMap.keySet()) {
			Predictor cloned = new Predictor(predictor);
			cloned.setChosenBaseTarget(target);
			Predictor evicted = heapMap.get(target).add(cloned);
			if (evicted != null) evictionList.add(evicted);
		}

		// Check that the evicted predictors are not present in any queue
		List<Predictor> dropList = new ArrayList<>();
		for (Predictor evicted : evictionList) {
			if (evicted!=null && !containsPredictor(evicted)) dropList.add(evicted);
		}

		// Return a unique list
		Set<Predictor> set = new HashSet<>(dropList);
		List<Predictor> result = new ArrayList<>(set);

		return result;
	}

	// Add predictor into a single heap.
	// Useful for target specific predictors like WoE.
	// Returns a list, which is either empty or with a single predictor
	public List<Predictor> addPredictor(Predictor predictor, String target) {
		List<Predictor> dropList = new ArrayList<>();

		// Put the predictor into a single queue
		predictor.setChosenBaseTarget(target);
		Predictor evicted = heapMap.get(target).add(predictor);

		// Check that the evicted predictor is not present in any queue
		if (evicted!=null && !containsPredictor(evicted)) dropList.add(evicted);

		return dropList;
	}

	// Return the top predictors for the given target
	public List<Predictor> getTopPredictors(String targetColumn) {

		List<Predictor> result = heapMap.get(targetColumn).getAll();
		int upperBound = Math.min(result.size(), featureLimitCount);

		return result.subList(0, upperBound);
	}

	// Return a unique list of all the predictors regardless of the target.
	// The count can be bigger than setting.featureLimitCount.
	public List<Predictor> getAllTopPredictors() {
		SortedSet<Predictor> set = new TreeSet<>();

		for (HeapWithFixedSize<Predictor> heap : heapMap.values()) {
			set.addAll(heap.getAll());
		}
		List<Predictor> result = new ArrayList<>(set);

		return result;
	}


	/////// Subroutines

	// Is this predictor one of the top N for at least one target?
	protected boolean containsPredictor(Predictor predictor) {
		for (HeapWithFixedSize<Predictor> heap : heapMap.values()) {
			if (heap.contains(predictor)) return true;
		}
		return false;
	}


}
