/**
 * A log of transactions between the database and Predictor Factory.
 * Example usages: recovery from connection loss, state space search optimization,...
 */
package extraction;

import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import run.Setting;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.File;
import java.time.LocalDateTime;
import java.util.*;


@XmlRootElement(name = "journal")
public class Journal {
	// Logging
	private static final Logger logger = Logger.getLogger(Journal.class.getName());

	private List<Predictor> journal = new ArrayList<>();                  // A complete list of calculated features
	private int expectedPredictorCount;                 // Useful for tracking the progress
	private int featureLimitCount;                      // Defines the N in journalTopN
	private Map<Integer, Predictor> groupIdDuplicate = new HashMap<>();   // Holds the best predictor per each groupId
	private Map<String, Predictor> valueDuplicate = new HashMap<>();      // Holds the fastest predictor per duplicate group


	// Constructor
	public Journal(@NotNull Setting setting, int expectedSize) {

		journal = new ArrayList<>(expectedSize); // Not a priority queue because I want to access elements at (N+1) position
		expectedPredictorCount = expectedSize;
		featureLimitCount = setting.predictorMax;

		// Create journal table in the database
		setting.dialect.getJournalPredictor(setting);

		// Log it
		logger.debug(expectedSize + " predictors are scheduled for calculation.");
	}

	// Constructor for JAXB
	public Journal() {}

	// Get journal size.
	public int size() {
		return journal.size();
	}

	// Get next predictor's id
	public int getNextId(@NotNull Setting setting) {
		return setting.predictorStart + journal.size() + 1;
	}

	// Add predictor
	public void addPredictor(@NotNull Setting setting, @NotNull Predictor predictor) {

		// Update the predictor's "delivered" timestamp
		predictor.setTimestampDelivered(LocalDateTime.now());

		// Mark bad predictors for delete
		if (!predictor.isOk()) predictor.setCandidateState(-1);

		// Checks for duplicates (functions with side effects)
		valueDuplicate(setting, predictor);
		groupIdDuplicate(setting, predictor);

		// Add the predictor to the journal
		journal.add(predictor);

		// Sort by {isCandidate, relevance, runtime}. The complexity for almost sorted list with TimSort is ~n.
		journal.sort(Predictor.RelevanceComparator);

		// Synchronize journal table in the database
		setting.dialect.addToJournalPredictor(setting, predictor);

		// Drop already unnecessary tables from the database
		dropToDrop(setting);

	}

	// Return the best predictors
	@NotNull public List<Predictor> getTopPredictors() {
		int len = 0;

		for (Predictor predictor : journal) {
			if (predictor.getCandidateState() != 1 || len == featureLimitCount) {
				break;
			} else {
				len++;
			}
		}

		return journal.subList(0, len);
	}

	///// Generic getters and setters
	public int getExpectedPredictorCount() {
		return expectedPredictorCount;
	}


	////// Subroutines
	private void groupIdDuplicate(@NotNull Setting setting, @NotNull Predictor challenger) {

		// Consider only good predictors
		if (challenger.getCandidateState() != 1) return;

		// Initialization
		int groupId = challenger.getGroupId();
		Predictor current = groupIdDuplicate.get(groupId);

		// If a new predictor, just add it
		if (current == null) {
			groupIdDuplicate.put(groupId, challenger);
			return;
		}

		// Else, compare the challenger and the current best
		double challengerRelevance = challenger.getRelevance(setting.baseTarget);
		double currentRelevance = groupIdDuplicate.get(groupId).getRelevance(setting.baseTarget);

		if (challengerRelevance > currentRelevance) {
			groupIdDuplicate.put(groupId, challenger);
			current.setCandidateState(-1);
		} else {
			challenger.setCandidateState(-1);
		}
	}

	// Compare based on the identity of {relevance, rowCount, nullCount}.
	private void valueDuplicate(@NotNull Setting setting, @NotNull Predictor challenger) {

		// Consider only good challengers
		if (challenger.getCandidateState() != 1) return;

		// Get predictor's hash
		Double relevance = challenger.getRelevance(setting.baseTarget);
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

	private void dropToDrop(@NotNull Setting setting) {

		// Generate an iterator. Start just after the last element.
		ListIterator li = journal.listIterator(journal.size());

		// Iterate in reverse.
		while (li.hasPrevious()) {
			Predictor predictor = (Predictor) li.previous();
			if (predictor.getCandidateState() == -1) {
				setting.dialect.dropTable(setting, predictor.getOutputTable());
				predictor.setCandidateState(0);
			} else {
				break;
			}
		}

		// Drop the (N+1)th, if necessary
		if (journal.size() > featureLimitCount && journal.get(featureLimitCount).getCandidateState() == 1) {
			Predictor overflower = journal.get(featureLimitCount);
			setting.dialect.dropTable(setting, overflower.getOutputTable());
			overflower.setCandidateState(0);
		}
	}


	////// JAXB

	// Load property list from XML
	@Nullable public static Journal unmarshall() {
		Journal list = null;

		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(Journal.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			list = (Journal) jaxbUnmarshaller.unmarshal(new File("config/journal.xml"));
		} catch (JAXBException e) {
			logger.warn("Attempt to parse 'config/journal.xml' failed. Does the file exist?");
			e.printStackTrace();
		}

		return list;
	}

	// Write into the XML
	public static void marshall(Journal journal) {

		try {
			File file = new File("config/journal.xml");
			JAXBContext jaxbContext = JAXBContext.newInstance(Journal.class);
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

			// Output pretty printed
			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

			// Write into the file
			jaxbMarshaller.marshal(journal, file);
		} catch (JAXBException e) {
			logger.warn("Attempt to write 'config/journal.xml' failed. Does Predictor Factory have the right to write?");
			e.printStackTrace();
		}
	}

}
