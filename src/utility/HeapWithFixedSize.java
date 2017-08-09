package utility;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.*;

// We can put inside anything that is comparable.
// If eviction happens, we return the evicted item. Otherwise we return null.
// Implementation details: Guava's MinMaxPriorityQueue follows the collection contract --> add() returns boolean.
// And that is not what we want.
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class HeapWithFixedSize<T> {
	private final int maxSize;
	private final PriorityQueue<T> priorityQueue;

	// Constructor
	public HeapWithFixedSize(int maxSize, Comparator comparator) {
		priorityQueue = new PriorityQueue<>(maxSize, Collections.reverseOrder(comparator)); // Reversed to keep the worst predictors at the top
		this.maxSize = maxSize;
	}

	// Private constructor for JAXB (the final values are overwritten by JAXB)
	private HeapWithFixedSize() {
		maxSize = -1;
		priorityQueue = null;
	}

	// Add the item while preserving the size limit.
	// For clarity, no optimization was performed (we always add the item...).
	// Note: It is ugly that we return null. Return of a singleton list would be nicer.
	public T add(T item) {
		priorityQueue.add(item);
		if (priorityQueue.size()>maxSize) return priorityQueue.poll(); // Return the evicted item
		return null;
	}

	// Returns a collection with all the items sorted in the descending order (from the best predictor to the worst)
	public List<T> getAll() {
		List<T> result = new ArrayList<>();
		result.addAll(priorityQueue);
		Collections.reverse(result);
		return result;
	}

	public int size() {
		return priorityQueue.size();
	}

	public T poll() {
		return priorityQueue.poll();
	}

	public boolean contains(T t) {
		return priorityQueue.contains(t);
	}

	@Override public String toString() {
		return "size=" + priorityQueue.size();
	}
}
