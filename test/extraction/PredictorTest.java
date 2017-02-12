package extraction;


import meta.MetaOutput;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import run.Setting;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class PredictorTest {

	private Setting setting = new Setting();
	private Pattern pattern = new Pattern();
	private Predictor predictor;
		
	@Before
	public void init() {
		pattern.name = "Pattern name";
		pattern.dialectCode = "select 3";
		pattern.author = "Thorough Tester";
		pattern.description = "Once upon a time...";
		pattern.cardinality = "1";
	}
	
	@Test
	public void setParameterMap() {
		predictor = new Predictor(pattern);
		predictor.setParameter("key2", "value4");
		
		Assert.assertEquals("select 3", predictor.getPatternCode());
		Assert.assertEquals("value4", predictor.getParameterMap().get("key2"));
	}

	@Test
	public void comparatorTwoDoubles() {
		// Initialization
		List<Predictor> predictorList = new ArrayList<>();
		
		predictor = new Predictor(pattern);
		predictor.setId(1);
		predictor.setRelevance("t", 0.5);
		predictorList.add(predictor);
		
		Predictor predictor2 = new Predictor(pattern);
		predictor2.setId(2);
		predictor2.setRelevance("t", 0.8);
		predictorList.add(predictor2);
		
		// Sort the relevances in descending order
		Collections.sort(predictorList, Predictor.RelevanceComparator);
		
		Assert.assertEquals(2, predictorList.get(0).getId());
		Assert.assertEquals(1, predictorList.get(1).getId());
	}


	@Test
	public void trimTo() throws UnsupportedEncodingException {
		int limit = 4;
		String actual = Predictor.trimTo("_čččččččččččččččč", limit);

		System.out.println(actual);
		System.out.println(actual.length());
		System.out.println(actual.getBytes("UTF-8").length);
		Assert.assertTrue(actual.length() <= limit);
		Assert.assertTrue(actual.getBytes("UTF-8").length <= limit);
	}

	@Test
	public void nameLength_unicode() throws UnsupportedEncodingException {
		// Initialization
		setting.identifierLengthMax = 64;

		predictor = new Predictor(pattern);
		predictor.setId(1);
		MetaOutput.OutputTable table = new MetaOutput.OutputTable();
		table.originalName = "tabčččččččččččččččččččččččččččččččččččččččččččččč";
		predictor.setTable(table);
		predictor.setParameter("par1", "valřřřřřřřřřřřřřřřřřřřřřřřř");
		predictor.setParameter("par2", "valžžžžžžžžžžžžžžžžžžžžžžžž");

		// Run
		String actual = predictor.getNameOnce(setting);

		System.out.println(actual);
		System.out.println(actual.length());
		System.out.println(actual.getBytes("UTF8").length);
		Assert.assertTrue(actual.length() <= setting.identifierLengthMax);
		Assert.assertTrue(actual.getBytes("UTF8").length <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_64_parameter() {
		// Initialization
		setting.identifierLengthMax = 64;
		
		predictor = new Predictor(pattern);
		predictor.setId(1);
		MetaOutput.OutputTable table = new MetaOutput.OutputTable();
		table.originalName = "table1";
		predictor.setTable(table);
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		Assert.assertTrue(actual.length() <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_64_2parameters() {
		// Initialization
		setting.identifierLengthMax = 64;
		
		predictor = new Predictor(pattern);
		predictor.setId(1);
		MetaOutput.OutputTable table = new MetaOutput.OutputTable();
		table.originalName = "table1";
		predictor.setTable(table);
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		Assert.assertTrue(actual.length() <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_64_patternName() {
		// Initialization
		setting.identifierLengthMax = 64;
		
		Pattern pattern = new Pattern();
		pattern.name = "pattern89_123456789_123456789_123456789_123456789_123456789";
		pattern.dialectCode = "select 3";
		pattern.author = "Thorough Tester";
		pattern.cardinality = "1";
		pattern.description = "Once upon a time...";
		
		predictor = new Predictor(pattern);
		predictor.setId(1);
		MetaOutput.OutputTable table = new MetaOutput.OutputTable();
		table.originalName = "table1";
		predictor.setTable(table);
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		Assert.assertTrue(actual.length() <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_64_table() {
		// Initialization
		setting.identifierLengthMax = 64;
		
		predictor = new Predictor(pattern);
		predictor.setId(1);
		MetaOutput.OutputTable table = new MetaOutput.OutputTable();
		table.originalName = "tab456789_123456789_123456789_123456789_123456789_123456789";
		predictor.setTable(table);
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		Assert.assertTrue(actual.length() <= setting.identifierLengthMax);
	}


	@Test
	public void nameLength_30_2parameters() {
		// Initialization
		setting.identifierLengthMax = 30;
		
		predictor = new Predictor(pattern);
		predictor.setId(1);
		MetaOutput.OutputTable table = new MetaOutput.OutputTable();
		table.originalName = "table1";
		predictor.setTable(table);
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		Assert.assertTrue(actual.length() <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_30_patternName() {
		// Initialization
		setting.identifierLengthMax = 30;
		
		Pattern pattern = new Pattern();
		pattern.name = "pattern89_123456789_123456789_123456789_123456789_123456789";
		pattern.dialectCode = "select 3";
		pattern.author = "Thorough Tester";
		pattern.cardinality = "1";
		pattern.description = "Once upon a time...";
		
		predictor = new Predictor(pattern);
		predictor.setId(1);
		MetaOutput.OutputTable table = new MetaOutput.OutputTable();
		table.originalName = "table1";
		predictor.setTable(table);
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		Assert.assertTrue(actual.length() <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_30_table() {
		// Initialization
		setting.identifierLengthMax = 30;
		
		predictor = new Predictor(pattern);
		predictor.setId(1);
		MetaOutput.OutputTable table = new MetaOutput.OutputTable();
		table.originalName = "tab456789_123456789_123456789_123456789_123456789_123456789";
		predictor.setTable(table);
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		Assert.assertTrue(actual.length() <= setting.identifierLengthMax);
	}
}
