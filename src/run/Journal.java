/** 
 * A log of transactions between the database and Predictor Factory.
 * Example usages: recovery from connection loss, state space search optimization,...
 */
package run;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import connection.SQL;
import featureExtraction.Predictor;

//Defines root element of the XML file
@XmlRootElement
//Defines order in which elements are created in XML file
@XmlType(propOrder = { "journal" })
public class Journal {

	private List<Predictor> journal;
	
	
	// Constructor
	public Journal(Setting setting) {

		journal = new ArrayList<>();
		
		// Create journal table in the database
		// I SHOULD REUSE THE OLD JOURNAL AND NOT JUST PLAINLY DELETE IT 
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
		
		// Add the predictor to the journal
		journal.add(predictor);
		
		// Synchronize journal table in the database
		SQL.addToJournal(setting, predictor);
	}

	
	
	
	///// Getters and setters
	public List<Predictor> getJournal() {
		return journal;
	}
	
	public void setJournal(List<Predictor> journal) {
		this.journal = journal;
	}
	
	
	
}
