package utility;

import org.junit.Assert;
import org.junit.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
		Assert.assertThat("sorted", Arrays.asList(1, 2, 3), is(heap.getAll()));
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

}
