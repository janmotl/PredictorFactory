package utility;

import extraction.Predictor;
import mother.PredictorMother;
import org.junit.Test;
import run.Setting;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// Passed Zester test
public class HeapMapTest {

	@Test
	public void modification() {
		Setting setting = new Setting("MariaDB", "mutagenesis_multiple");
		setting.targetColumnList = Arrays.asList("mutagenic", "ind1");
		HeapMap heapMap = new HeapMap(setting.targetColumnList, Predictor.SingleRelevanceComparator, 2);
		List<Predictor> returned;

		assertEquals(0, heapMap.getTopPredictors("mutagenic").size());
		assertEquals(0, heapMap.getTopPredictors("ind1").size());

		returned = heapMap.addPredictor(PredictorMother.woeMutagenic(), "mutagenic");   // A single target [0.5 0]
		assertTrue(returned.isEmpty());
		assertEquals(1, heapMap.getTopPredictors("mutagenic").size());
		assertEquals(0, heapMap.getTopPredictors("ind1").size());

		returned = heapMap.addPredictor(PredictorMother.aggregateMax());                // All targets  [0.7 0.2]
		assertTrue(returned.isEmpty());
		assertEquals(2, heapMap.getTopPredictors("mutagenic").size());
		assertEquals(1, heapMap.getTopPredictors("ind1").size());

		returned = heapMap.addPredictor(PredictorMother.aggregateMin());                // [0.6 0.1]
		assertEquals("woeMutagenic", returned.get(0).getName());
		assertEquals(2, heapMap.getTopPredictors("mutagenic").size());                  // The limit should apply
		assertEquals(2, heapMap.getTopPredictors("ind1").size());

		returned = heapMap.addPredictor(PredictorMother.aggregateAvg());                // [0.4 0.9]
		assertTrue(returned.isEmpty());
		assertEquals(2, heapMap.getTopPredictors("mutagenic").size());
		assertEquals(2, heapMap.getTopPredictors("ind1").size());

		assertEquals(3, heapMap.getAllTopPredictors().size());                          // Min, Max, Avg

		returned = heapMap.addPredictor(PredictorMother.woeInd1(), "ind1");             // A single target [0 0.8]
		assertTrue(returned.isEmpty());
		assertEquals(2, heapMap.getTopPredictors("mutagenic").size());
		assertEquals(2, heapMap.getTopPredictors("ind1").size());

		assertEquals(4, heapMap.getAllTopPredictors().size());   // Min, Max, Avg, woeInd1
    }

	@Test
	public void containsOnClonedPredictors() {
		Setting setting = new Setting("MariaDB", "mutagenesis_multiple");
		setting.targetColumnList = Arrays.asList("mutagenic", "ind1");
		HeapMap heapMap = new HeapMap(setting.targetColumnList, Predictor.SingleRelevanceComparator, 2);

		Predictor predictor = PredictorMother.woeMutagenic();
		heapMap.addPredictor(PredictorMother.woeMutagenic());
		assertTrue(heapMap.containsPredictor(predictor)); // Check for the same object

		Predictor clone = new Predictor(predictor);
		assertTrue(heapMap.containsPredictor(clone)); // Check for the clone

		Predictor another = PredictorMother.aggregateAvg();
		assertFalse(heapMap.containsPredictor(another)); // Check for a completely different object
	}

	@Test
	public void serialization() throws JAXBException {
		Setting setting = new Setting("MariaDB", "mutagenesis_multiple");
		setting.targetColumnList = Arrays.asList("mutagenic", "ind1");
		HeapMap original = new HeapMap(setting.targetColumnList, Predictor.SingleRelevanceComparator, 2);

		original.addPredictor(PredictorMother.woeMutagenic());  //  0.5 0   woeMutagenic
		original.addPredictor(PredictorMother.woeInd1());       //  0   0.8 woeInd1
		original.addPredictor(PredictorMother.aggregateMax());  //  0.7 0.2 aggregateMax
		original.addPredictor(PredictorMother.aggregateMin());

		// Marshalling
		JAXBContext context = JAXBContext.newInstance(original.getClass());
		Marshaller marshaller = context.createMarshaller();
		StringWriter writer = new StringWriter();
		marshaller.marshal(original, writer);
		String outString = writer.toString();

		assertTrue(outString.contains("</heapMap"));

		// Unmarshalling
		context = JAXBContext.newInstance(HeapMap.class);
		Unmarshaller unmarshaller = context.createUnmarshaller();
		StringReader reader = new StringReader(outString);
		HeapMap received = (HeapMap) unmarshaller.unmarshal(reader);

		assertEquals(original.getAllTopPredictors(), received.getAllTopPredictors());
	}
}
