/** 
 * A log of transactions between the database and Predictor Factory.
 * Example usages: recovery from connection loss, state space search optimization,...
 */
package featureExtraction;

import connection.SQL;
import run.Setting;

import java.time.LocalDateTime;
import java.util.*;


public class Journal {

	private List<Predictor> journal;				// A complete list of calculated features
	private final int expectedPredictorCount;		// Useful for tracking the progress
	private final int featureLimitCount;			// Defines the N in journalTopN
	private Map<Integer, Predictor> groupIdDuplicate;	// Holds the best predictor per each groupId
	private Map<String, Predictor> valueDuplicate;	// Holds the fastest predictor per duplicate group
	
	
	// Constructor
	public Journal(Setting setting, int expectedSize) {

		journal = new ArrayList<>(expectedSize); // Not a priority queue because I want to access elements at (N+1) position
		expectedPredictorCount = expectedSize;
		featureLimitCount = setting.predictorMax;
		groupIdDuplicate = new HashMap<>();
		valueDuplicate = new HashMap<>();
		
		// Create journal table in the database
		SQL.getJournal(setting);
	}
	 
	// Get journal size.
	public int size(){
		return journal.size();
	}
	
	// Get next predictor's id
	public int getNextId(Setting setting) {
		return setting.predictorStart + journal.size() + 1;
	}
	
	// Add predictor
	public void addPredictor(Setting setting, Predictor predictor){

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
		SQL.addToJournal(setting, predictor);

		// Drop already unnecessary tables from the database
		dropToDrop(setting);

	}

	// Return the best predictors
	public Collection<Predictor> getTopPredictors() {
		int len = 0;

		for (Predictor predictor : journal) {
			if (predictor.getCandidateState()!=1 || len==featureLimitCount) {
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

	private void groupIdDuplicate(Setting setting, Predictor challenger) {

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
	private void valueDuplicate(Setting setting, Predictor challenger) {

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

	private void dropToDrop(Setting setting) {

		// Generate an iterator. Start just after the last element.
		ListIterator li = journal.listIterator(journal.size());

		// Iterate in reverse.
		while(li.hasPrevious()) {
			Predictor predictor = (Predictor) li.previous();
			if (predictor.getCandidateState() == -1) {
				SQL.dropTable(setting, predictor.outputTable);
				predictor.setCandidateState(0);
			}
			else {
				break;
			}
		}

		// Drop the (N+1)th, if necessary
		if (journal.size()>featureLimitCount && journal.get(featureLimitCount).getCandidateState()==1) {
			Predictor overflower = journal.get(featureLimitCount);
			SQL.dropTable(setting, overflower.outputTable);
			overflower.setCandidateState(0);
		}
	}

}
