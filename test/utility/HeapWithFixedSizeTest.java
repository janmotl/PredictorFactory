package utility;

import extraction.Predictor;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

// Passed Zester test
public class HeapWithFixedSizeTest {

	@Test
	public void modification() {
		HeapWithFixedSize<Integer> heap = new HeapWithFixedSize<>(3, null); // We are using default comparator
		heap.add(1);
		heap.add(5);
		Integer evicted3 = heap.add(3);
		Integer evicted5 = heap.add(2);
		Integer evicted7 = heap.add(7);

		assertTrue("no eviction expected", evicted3 == null);
		assertTrue("eviction of an old item", evicted5 == 5);
		assertTrue("eviction of the currently added item", evicted7 == 7);
		assertEquals("limited size", 3, heap.size());
		Assert.assertThat("sorted", Arrays.asList(1, 2, 3), is(heap.getAllSorted(null)));
		assertTrue("poll", heap.poll() == 3);
		assertTrue(heap.contains(1));
		assertFalse(heap.contains(5));
	}

	@Test
	public void marshalling() throws JAXBException {
		HeapWithFixedSize<String> original = new HeapWithFixedSize<>(3, null);
		original.add("item1");
		original.add("item2");

		// Marshalling
		JAXBContext context = JAXBContext.newInstance(original.getClass());
		Marshaller marshaller = context.createMarshaller();
		StringWriter writer = new StringWriter();
		marshaller.marshal(original, writer);
		String outString = writer.toString();

		assertTrue(outString.contains("</heapWithFixedSize"));

		// Unmarshalling
		context = JAXBContext.newInstance(HeapWithFixedSize.class);
		Unmarshaller unmarshaller = context.createUnmarshaller();
		StringReader reader = new StringReader(outString);
		HeapWithFixedSize received = (HeapWithFixedSize) unmarshaller.unmarshal(reader);

		assertEquals(original.size(), received.size());
	}

    @Test
	public void getAllSorted() {
		HeapWithFixedSize<Integer> heap = new HeapWithFixedSize<>(10, null); // We are using default comparator

        List<Integer> data = IntStream.range(0, 64).boxed().collect(Collectors.toList());
        Collections.shuffle(data, new Random(2001));

        for (Integer integer : data) {
            heap.add(integer);
        }

        assertEquals("[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]", heap.getAllSorted(null).toString());
	}

    @Test
	public void getAllSorted_predictors() {
		HeapWithFixedSize<Predictor> heap = new HeapWithFixedSize<>(16, Predictor.SingleRelevanceComparator);
        Random rnd = new Random(2001);

        for (double i = 0; i < 25; i++) {
            Predictor predictor = new Predictor();
            predictor.setChosenBaseTarget("status");
            predictor.setRelevance("status", rnd.nextDouble());
            predictor.setConceptDrift("status", rnd.nextDouble());
            heap.add(predictor);
        }

        List<Predictor> result = heap.getAllSorted(Predictor.SingleRelevanceComparator);

        // Check that the weighted Relevance is monotonically descending
        double weightedRelevanceLagged = Double.MAX_VALUE;
        for (Predictor predictor : result) {
            assertTrue(weightedRelevanceLagged >= predictor.getWeightedRelevance("status"));
            weightedRelevanceLagged = predictor.getWeightedRelevance("status");
        }

	}

}
