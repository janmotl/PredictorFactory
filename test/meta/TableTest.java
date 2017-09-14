package meta;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TableTest {

	@Test
	public void getMatchingFKCs() throws Exception {
		Table t1 = new Table();
		t1.name = "t1";
		t1.schemaName = "schema";

		Table t2 = new Table();
		t2.name = "t2";
		t2.schemaName = "schema";

		Table t3 = new Table();
		t3.name = "t3";
		t3.schemaName = "schema";

		List<String> t1Columns = new ArrayList<>();
		t1Columns.add("t1c1");
		List<String> t2Columns = new ArrayList<>();
		t2Columns.add("t2c1");
		ForeignConstraint fc = new ForeignConstraint("name", "schema", "t2", "schema", "t1", t2Columns, t1Columns);
		t1.foreignConstraintList.add(fc);

		// Asserts
		assertEquals(0, t1.getMatchingFKCs(t3).size()); // t1 -/-> t3
		assertEquals(1, t1.getMatchingFKCs(t2).size()); // t1 ---> t2
		assertEquals(0, t2.getMatchingFKCs(t1).size()); // t2 -/-> t1
	}

}