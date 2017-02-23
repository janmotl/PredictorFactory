package meta;


import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class ForeignConstraintListTest {

	@Test
	public void testReadWriteRead_Mutagenesis() {

		ForeignConstraintList fc = ForeignConstraintList.unmarshall("foreignConstraint_Mutagenesis.xml");
		ForeignConstraintList.marshall(fc, "foreignConstraint_Mutagenesis.xml");
		ForeignConstraintList fc2 = ForeignConstraintList.unmarshall("foreignConstraint_Mutagenesis.xml");

		String expected = "molecule";
		String retrieved = fc.getForeignConstraintList("molecule").get(0).table;
		String retrieved2 = fc2.getForeignConstraintList("molecule").get(0).table;

		Assert.assertEquals(expected, retrieved);
		Assert.assertEquals(expected, retrieved2);
	}

	@Test
	public void testWriteRead_compositeKey() {

		ForeignConstraintList foreignConstraintList = new ForeignConstraintList();
		foreignConstraintList.schemaName = "schema";
		foreignConstraintList.databaseName = "database";
		foreignConstraintList.foreignConstraint = new ArrayList<>();

		ForeignConstraint foreignConstraint = new ForeignConstraint();
		foreignConstraint.table = "table";
		foreignConstraint.fTable = "fTable";
		foreignConstraint.column = new ArrayList<>();
		foreignConstraint.column.add("col1_1");
		foreignConstraint.column.add("col1_2");
		foreignConstraint.fColumn = new ArrayList<>();
		foreignConstraint.fColumn.add("col1_1");
		foreignConstraint.fColumn.add("col1_2");
		foreignConstraintList.foreignConstraint.add(foreignConstraint);

		ForeignConstraint foreignConstraint2 = new ForeignConstraint();
		foreignConstraint2.table = "table2";
		foreignConstraint2.fTable = "fTable2";
		foreignConstraint2.column = new ArrayList<>();
		foreignConstraint2.column.add("col2_1");
		foreignConstraint2.column.add("col2_2");
		foreignConstraint2.fColumn = new ArrayList<>();
		foreignConstraint2.fColumn.add("col2_1");
		foreignConstraint2.fColumn.add("col2_2");
		foreignConstraintList.foreignConstraint.add(foreignConstraint2);

		// Run
		ForeignConstraintList.marshall(foreignConstraintList, "foreignConstraint_compositeKey.xml");
		ForeignConstraintList fc = ForeignConstraintList.unmarshall("foreignConstraint_compositeKey.xml");

		// Comparison
		String expected = "col1_1";
		String retrieved = fc.getForeignConstraintList("table").get(0).column.get(0);

		Assert.assertEquals(expected, retrieved);

	}

	@Test
	public void equal() {
		ForeignConstraint foreignConstraint = new ForeignConstraint();
		foreignConstraint.table = "table";
		foreignConstraint.fTable = "fTable";
		foreignConstraint.column = new ArrayList<>();
		foreignConstraint.column.add("col1_1");
		foreignConstraint.column.add("col1_2");
		foreignConstraint.fColumn = new ArrayList<>();
		foreignConstraint.fColumn.add("col1_1");
		foreignConstraint.fColumn.add("col1_2");

		ForeignConstraint foreignConstraint2 = new ForeignConstraint();
		foreignConstraint2.table = "table";
		foreignConstraint2.fTable = "fTable";
		foreignConstraint2.column = new ArrayList<>();
		foreignConstraint2.column.add("col2_1");
		foreignConstraint2.column.add("col2_2");
		foreignConstraint2.fColumn = new ArrayList<>();
		foreignConstraint2.fColumn.add("col2_1");
		foreignConstraint2.fColumn.add("col2_2");

		Assert.assertFalse(foreignConstraint.equals(foreignConstraint2));
		foreignConstraint.name="name";
		Assert.assertFalse(foreignConstraint.equals(foreignConstraint2));
		Assert.assertFalse(foreignConstraint2.equals(foreignConstraint));
		foreignConstraint2.name="name";
		Assert.assertTrue(foreignConstraint.equals(foreignConstraint2));
	}
}