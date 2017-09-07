package utility;


import org.junit.Assert;
import org.junit.Test;
import run.Setting;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TextParserTest {
	private static final Setting setting = new Setting("PostgreSQL", "financial");

	@Test
	public void list2mapSingle() {
		List<String> list = new ArrayList<>();
		list.add("table.column");
		Map<String, List<String>> actual = TextParser.list2map(list, setting.targetSchema);

		Assert.assertEquals("{table=[column]}", actual.toString());
    }

	@Test
	public void list2mapMultiple() {
		List<String> list = new ArrayList<>();
		list.add("table.column");
		list.add("table.column2");
		Map<String, List<String>> actual = TextParser.list2map(list, setting.targetSchema);

		Assert.assertEquals("{table=[column, column2]}", actual.toString());
	}

	@Test
	public void list2mapMalformed() {
		List<String> list = new ArrayList<>();
		list.add("a.b.c.d.column");
		Map<String, List<String>> actual = TextParser.list2map(list, setting.targetSchema);

		Assert.assertEquals("{}", actual.toString());
	}

	@Test
	public void list2mapMap() {
		List<String> list = new ArrayList<>();
		list.add("schema.table.column");
		list.add("schema.table.column2");
		Map<String, Map<String, List<String>>> actual = TextParser.list2mapMap(list, setting.targetSchema);

		Assert.assertEquals("{schema={table=[column, column2]}}", actual.toString());
	}

	@Test
	public void list2mapMap_complex() {
		List<String> list = new ArrayList<>();
		list.add("schema1.table1.column1");
		list.add("schema1.table1.column2");
		list.add("schema1.table2.column1");
		list.add("schema2.table1.column1");
		list.add("schema2.table2.column1");
		list.add("schema2.table1.column2");
		list.add("schema1.table2.column2");
		list.add("schema2.table2.column2");
		Map<String, Map<String, List<String>>> actual = TextParser.list2mapMap(list, setting.targetSchema);

		Assert.assertEquals("{schema2={table2=[column1, column2], table1=[column1, column2]}, schema1={table2=[column1, column2], table1=[column1, column2]}}", actual.toString());
	}

}
