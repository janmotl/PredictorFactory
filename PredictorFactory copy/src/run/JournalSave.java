package run;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;



public class JournalSave {

	
	public static void main(String[] args) {
		String FILE_NAME = "src/resources/journal.xml";
		Setting setting = new Setting();
		Journal journal = new Journal(setting);
		journal2xml(journal, FILE_NAME);

	}
	
	public static void journal2xml(Journal journal, final String FILE_NAME) {
		try {
			JAXBContext context = JAXBContext.newInstance(Journal.class);
			Marshaller m = context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

			// Write to File
			m.marshal(journal, new File(FILE_NAME));
		} catch (JAXBException e) {
			e.printStackTrace();
		}
	}

}
