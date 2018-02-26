package extraction;

import org.apache.log4j.Logger;
import run.Setting;
import utility.HeapWithFixedSize;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.File;
import java.time.LocalDateTime;
import java.util.*;

// Class to keep the count of returned predictors upper-bounded because the count of columns in a single table
// is commonly limited (e.g. Oracle has a limit of 1000 columns per table).
@XmlRootElement(name = "journal")
public class Journal {
	// Logging
	private static final Logger logger = Logger.getLogger(Journal.class.getName());

	// JAXB metadata - to be able to validate that journal.xml matches the current setting.
	// These attributes are used by {marshaller() and unmarshaller()}.
	// There is a plethora of other attributes I should take care of like lag and lead.
	// BUT there are also variables that I should ignore like isExploitationStage -> I cannot simply calculate a hash.
	@XmlAttribute private String databaseVendor;    // This check is important during unit testing, when we test the same schema against multiple vendors
	@XmlAttribute private String inputSchemaList;   // Each inputSchema contains different tables
	@XmlAttribute private String targetTable;       // We should be able to uniquely identify the target(s)
	@XmlAttribute private String targetColumnList;  // The selection of the predictors is valid only for the selected targets
	@XmlAttribute private String targetIdList;      // The values of the predictors can be completely different based on the specified targetIds
	@XmlAttribute private String targetDate;        // The reference times should not differ

	// Variables
	private final Map<String, HeapWithFixedSize<Predictor>> heapMap = new HashMap<>();  // Map: baseTarget -> queue
	private Map<String, Predictor> groupIdDuplicate = new HashMap<>();  // Holds the best predictor per {groupId, target}
	private Map<String, Predictor> valueDuplicate = new HashMap<>();    // Holds the fastest predictor per duplicate group
	private int featureLimitCount;                                      // Defines the N in journalTopN
	private int evaluatedCount = 0;                                     // The count of added predictors (for progressbar)


	// Constructor
	public Journal(Setting setting, int expectedSize) {
		featureLimitCount = setting.predictorMax;

		// We keep a map<baseTarget, queue> for fast resorting
		for (String baseTarget : setting.baseTargetList) {
			heapMap.put(baseTarget, new HeapWithFixedSize<>(featureLimitCount, Predictor.SingleRelevanceComparator));
		}

		// Create journal table in the database
		setting.dialect.getJournalPredictor(setting);

		// Log it
		logger.debug(expectedSize + " predictors are scheduled for calculation.");
	}

	// Constructor for JAXB
	public Journal() {}

	// Add a single predictor.
	// Returns a list of dropped predictors (list because we permit multiple targets and a single predictor may evict
	// predictors from multiple heaps).
	public LinkedHashSet<Predictor> addPredictor(Setting setting, Predictor predictor) {
		// Increment counter (we need the counter because the storage may evict useless predictors)
		evaluatedCount++;

		// Update the predictor's "delivered" timestamp
		predictor.setTimestampDelivered(LocalDateTime.now());

		// Mark bad predictors for delete
		if (!predictor.isOk()) predictor.setCandidateState(-1);

		// Checks for duplicates (functions with side effects)
		valueDuplicate(predictor);
		groupIdDuplicate(predictor);

		// Synchronize journal table in the database
		setting.dialect.addToJournalPredictor(setting, predictor);

		// Add the predictor into the correct queue (iff the predictor is a candidate).
		// Note: The implementation is not memory friendly as we make copies of large objects.
		// Before optimizing, check: Data Structures in Multi-Objective Evolutionary Algorithms (Altwaijry, 2012).
		// The conclusion is that for less than ~2500 items a simple linear array is faster than a tree.
		// Furthermore, if just two objectives are important, linear arrays are superior once again.
		// Hence, if we need to save memory, just implement it with an array (I know your programmer soul wants a
		// solution, which scale, but these are the data). Proposal: Have a list with the objects (Predictors) and an
		// array with the fitness. This way it is testable as we pass the Object together with the fitness vector.
		String baseTarget = predictor.getBaseTarget();
		List<Predictor> evictionList = new ArrayList<>();

		if (predictor.getCandidateState()>0) {
			if (baseTarget == null) {
				// BaseTarget agnostic --> put the predictor into each queue
				for (String target : setting.baseTargetList) {
					if (predictor.getWeightedRelevance(target) < 0.0001) {  // If the predictor is of no good, drop it.
						evictionList.add(predictor);                        // WeightedRelevance is double -> use inequality
					} else {
						Predictor cloned = new Predictor(predictor);
						cloned.setChosenBaseTarget(target);
						Predictor evicted = heapMap.get(target).add(cloned);
						if (evicted != null) evictionList.add(evicted);
					}
				}
			} else {
				// BaseTarget specific --> put the predictor into a single queue
				if (predictor.getWeightedRelevance(baseTarget) < 0.0001) {  // If the predictor is worthless, drop it
					evictionList.add(predictor);                            // WeightedRelevance is double -> use inequality
				} else {
					predictor.setChosenBaseTarget(predictor.getBaseTarget());
					Predictor evicted = heapMap.get(baseTarget).add(predictor);
					if (evicted != null) evictionList.add(evicted);
				}
			}
		}

		// Check that the evicted predictors are not present in any queue.
		// No duplicates are permitted.
		LinkedHashSet<Predictor> dropList = new LinkedHashSet<>();
		for (Predictor evictedPredictor : evictionList) {
			if (!containsPredictor(evictedPredictor)) dropList.add(evictedPredictor);
		}

		// Drop already unnecessary tables from the database
		for (Predictor dropPredictor : dropList) {
			dropPredictor(setting, dropPredictor);
		}

		return dropList;    // For testing purposes
	}

	// Return the top predictors for the given target
	public List<Predictor> getTopPredictors(String baseTarget) {

		List<Predictor> result = heapMap.get(baseTarget).getAllSorted(Predictor.SingleRelevanceComparator);
		int upperBound = Math.min(result.size(), featureLimitCount);

		return result.subList(0, upperBound);
	}

	// Return a unique list of all the predictors regardless of the target.
	// The count can be bigger than setting.featureLimitCount.
	public List<Predictor> getAllTopPredictors() {
		SortedSet<Predictor> set = new TreeSet();

		for (HeapWithFixedSize<Predictor> heap : heapMap.values()) {
			set.addAll(heap.getAll());
		}
		List<Predictor> result = new ArrayList<>(set);

		return result;
	}

	// Provided for logging purposes
	public int getEvaluationCount() {
		return evaluatedCount;
	}


	/////// Subroutines

	// Is this predictor one of the top N for at least one target?
	protected boolean containsPredictor(Predictor predictor) {
		for (HeapWithFixedSize<Predictor> heap : heapMap.values()) {
			if (heap.contains(predictor)) return true;
		}
		return false;
	}

	// Drop the predictor from the database to save storage space. Mark the predictor as useless in the journal
	private void dropPredictor(Setting setting, Predictor predictor) {
		if (predictor == null) return;

		setting.dialect.dropTable(setting, predictor.getOutputTable());
		predictor.setCandidateState(0);
	}

	// Identity is identified based on equality of {relevance, rowCount, nullCount}.
	// Output: Sets Predictor.candidateState variable.
	// Justification: We want to avoid duplicate features in the mainSample.
	private void valueDuplicate(Predictor challenger) {

		// Consider only good challengers
		if (challenger.getCandidateState() != 1) return;

		// Get predictor's hash
		String relevance = challenger.getRelevanceObject().toString();
		int rowCount = challenger.getRowCount();
		int nullCount = challenger.getNullCount();
		String hash = relevance + " " + rowCount + " " + nullCount;

		// Get the current best duplicate, if available
		Predictor current = valueDuplicate.get(hash);

		// If a new predictor, just add it
		if (current == null) {
			valueDuplicate.put(hash, challenger);
			return;
		}

		// Else, just throw away the new predictor.
		// We could compare the challenger with the current best duplicate based on runtime, but then we would have
		// to update isDuplicate information in the journal (or remove the information from the journal, or write
		// the information in a batch at the end of Predictor Factory).
		challenger.setInferiorDuplicate(true);
		challenger.setCandidateState(-1);
		challenger.setDuplicateName(current.getName());
	}

	// Keep a single best feature per groupId and baseTarget.
	// Output: Sets Predictor.candidateState variable.
	// Justification: We want to avoid of returning 1000 slight variations of a single predictor.
	protected List<Predictor> groupIdDuplicate(Predictor challenger) {
		List<Predictor> dropList = new ArrayList<>();

		// Consider only good predictors
		if (challenger.getCandidateState()>0) {
			if (challenger.getBaseTarget() == null) {
				// BaseTarget agnostic -> compare individually for each target, drop old if the new is everywhere superior, we may have to drop multiple predictors
				List<Predictor> dropCandidates = new ArrayList<>();
				for (String baseTarget : heapMap.keySet()) {
					String hash = challenger.getGroupId() + " " + baseTarget;   // Get predictor's hash
					Predictor current = groupIdDuplicate.get(hash);
					dropCandidates.addAll(groupIdDuplicateComparison(hash, baseTarget, current, challenger));
				}
				// We can drop only predictors which are not present in groupIdDuplicate
				for (Predictor predictor : dropCandidates) {
					if (!groupIdContains(predictor, challenger.getGroupId())) {
						dropList.add(predictor);
						predictor.setCandidateState(-1);
					}
				}
			} else {
				// BaseTarget specific -> compare just against the single target
				String baseTarget = challenger.getChosenBaseTarget();
				String hash = challenger.getGroupId() + " " + baseTarget;   // Get predictor's hash
				Predictor current = groupIdDuplicate.get(hash);
				dropList.addAll(groupIdDuplicateComparison(hash, baseTarget, current, challenger));
			}
		}

		return dropList;
	}

	private boolean groupIdContains(Predictor predictor, int groupId) {
		for (String baseTarget : heapMap.keySet()) {
			String hash = groupId + " " + baseTarget;
			if (groupIdDuplicate.get(hash).getId() == predictor.getId()) return true;
		}

		return false;
	}

	// Return a predictor to drop.
	private List<Predictor> groupIdDuplicateComparison(String hash, String baseTarget, Predictor current, Predictor challenger) {
		// Initialization
		List<Predictor> dropList = new ArrayList<>();

		if (current == null) {
			// If a new predictor, just add it
			groupIdDuplicate.put(hash, challenger);
		} else {
			// Else, compare the challenger and the current best based on {weightedRelevance}
			// Note: Could have also compared based on the runtime.
			double challengerRelevance = challenger.getWeightedRelevance(baseTarget);
			double currentRelevance = current.getWeightedRelevance(baseTarget);

			if (challengerRelevance > currentRelevance) {
				groupIdDuplicate.put(hash, challenger);
				current.setCandidateState(-1);
				dropList.add(current);
			} else {
				challenger.setCandidateState(-1);
				dropList.add(challenger);
			}
		}

		return dropList;
	}




	////// JAXB

	// Load the journal from XML and make sure the journal is compatible with the current setting
	public static Journal unmarshall(Setting setting) throws InstantiationException, JAXBException {
		logger.debug("Attempting to unmarshall journal.xml.");

		// Does journal.xml exist and does it appear to be a syntactically valid XML?
		JAXBContext jaxbContext = JAXBContext.newInstance(Journal.class);
		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		Journal journal = (Journal) jaxbUnmarshaller.unmarshal(new File("config/journal.xml"));

		// Do we perform two-phase processing?
		if (!setting.useTwoStages) throw new InstantiationException();

		// Are we at the beginning of the exploitation phase?
		if (!setting.isExploitationPhase) throw new InstantiationException();

		// Does the content of journal.xml match setting? (to avoid reading of unrelated XML)
		if (!(setting.databaseVendor.equals(journal.databaseVendor))) throw new InstantiationException();
		if (!(setting.inputSchemaList.toString().equals(journal.inputSchemaList))) throw new InstantiationException();
		if (!(setting.targetTable.equals(journal.targetTable))) throw new InstantiationException();
		if (!(setting.targetColumnList.toString().equals(journal.targetColumnList))) throw new InstantiationException();
		if (!(setting.targetIdList.toString().equals(journal.targetIdList))) throw new InstantiationException();
		if (setting.targetDate!=null && !(setting.targetDate.equals(journal.targetDate))) throw new InstantiationException();

		// Ok, journal.xml seems to be reusable
		logger.debug("The journal.xml was successfully unmarshalled.");
		return journal;
	}

	// Write this journal into the XML
	public void marshall(Setting setting) {

		// Set first metadata from the setting into this journal instance
		databaseVendor = setting.databaseVendor;
		inputSchemaList = setting.inputSchemaList.toString();
		targetTable = setting.targetTable;
		targetColumnList = setting.targetColumnList.toString();
		targetIdList = setting.targetIdList.toString();
		targetDate = setting.targetDate;

		// Ordinary marshalling
		try {
			File file = new File("config/journal.xml");
			JAXBContext jaxbContext = JAXBContext.newInstance(Journal.class);
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

			// Output pretty printed
			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

			// Write into the file
			jaxbMarshaller.marshal(this, file);
		} catch (JAXBException e) {
			logger.warn("Attempt to write 'config/journal.xml' failed. Does Predictor Factory have the right to write?");
			e.printStackTrace();
		}
	}
}
