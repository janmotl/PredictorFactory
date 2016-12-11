package utility;


import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TextParserTest {

	@Test
	public void list2mapSingle() {
		List<String> list = new ArrayList<>();
		list.add("table.column");
		Map<String, List<String>> actual = TextParser.list2map(list);

		Assert.assertEquals("{table=[column]}", actual.toString());
    }

	@Test
	public void list2mapMultiple() {
		List<String> list = new ArrayList<>();
		list.add("table.column");
		list.add("table.column2");
		Map<String, List<String>> actual = TextParser.list2map(list);

		Assert.assertEquals("{table=[column, column2]}", actual.toString());
	}

	@Test
	public void list2mapMalformed() {
		List<String> list = new ArrayList<>();
		list.add("column"); // Missing table name and dot
		Map<String, List<String>> actual = TextParser.list2map(list);

		Assert.assertEquals("{}", actual.toString());
	}

}
