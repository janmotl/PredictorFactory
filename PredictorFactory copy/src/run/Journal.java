package run;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import utility.Network;
import utility.SQL;

//Defines root element of the XML file
@XmlRootElement
//Defines order in which elements are created in XML file
@XmlType(propOrder = { "journal" })

// A log of transactions between the database and Predictor Factory.
// Example usages: recovery from connection loss, state space search optimization,...
public class Journal {

	
	//private Setting setting;
	private SortedSet<Predictor> journal;	// Wouldn't ArrayList be enough?
	
	private final long startTime = System.currentTimeMillis();
	



	// Constructor
	public Journal(Setting setting) {
	
		journal = new TreeSet<Predictor>();
		
		// Create journal table in the database
		// I SHOULD REUSE THE OLD JOURNAL AND NOT JUST PLAINLY DELETE IT 
		Network.executeUpdate(setting.connection, SQL.getJournal(setting));
		//setting.predictor_count = setting.predictor_start;
	}
	
	// Get journal size
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
		
		// Add the predictor to the journal
		journal.add(predictor);
		
		// Synchronize journal table in the database
		String sql = SQL.addToJournal(setting, predictor);
		Network.executeUpdate(setting.connection, sql);
	}

	// Get table names of all predictors 
	public ArrayList<String> getAllTables(){
		ArrayList<String> result = new ArrayList<String>();
		
		for (Predictor predictor : journal) {
			// Return only good predictors 
			if (predictor.isOk()) {
				result.add(predictor.outputTable);
			}	
		}
		
		return result;
	}
	
	// Get all column names
	public ArrayList<String> getAllColumns(){
		ArrayList<String> result = new ArrayList<String>();
		
		for (Predictor predictor : journal) {
			// Return only good predictors 
			if (predictor.isOk()) {
				result.add(predictor.getName());
			}	
		}
		
		return result;
	}
	
	// Getters and setters
	public SortedSet<Predictor> getJournal() {
		return journal;
	}
	
	public void setJournal(SortedSet<Predictor> journal) {
		this.journal = journal;
	}
	
	// Get wall-clock time from starting journal
	public long getRunTime(){
		return System.currentTimeMillis() - startTime;
	}
}
