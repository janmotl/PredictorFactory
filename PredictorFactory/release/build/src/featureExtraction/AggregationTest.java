package featureExtraction;

import metaInformation.MetaOutput;
import metaInformation.MetaOutput.OutputTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import run.Setting;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class AggregationTest {
	private Setting setting = new Setting("PostgreSQL", "financial");
	private Pattern pattern = new Pattern();
	private Predictor predictor;
	private SortedMap<String, OutputTable> tableMetadata = new TreeMap<String, MetaOutput.OutputTable>();
	
	@Before
	public void initialize(){
		
		// Initialize pattern
		SortedMap<String, String> parameter = new TreeMap<String, String>();
		parameter.put("parameterName1", "parameterValue1,parameterValue2");
		parameter.put("parameterName2", "parameterValueA,parameterValueB");
		pattern.dialectParameter = parameter;
		pattern.name = "patternName";
		pattern.dialectCode = "select @nominalColumn from t1 where col1 = '@value' and col2 = '@targetValue'";
		pattern.author = "Majestic Tester";
		pattern.cardinality = "1";
		
		// Initialize predictor
		predictor = new Predictor(pattern);
		predictor.setSql("select * from t1 where propagated_target = '@targetValue'");
		predictor.propagatedTable = "propagatedTable1";
		predictor.columnMap.put("@nominalColumn", "column1");
		predictor.setParameter("@value", "value1");
		
		// Initialize tableMetadata
		OutputTable outputTable = new OutputTable();
		outputTable.propagatedName = "propagatedName1"; 
		SortedMap<String, List<String>> uniqueList = new TreeMap<String, List<String>>();
		ArrayList<String> unique = new ArrayList<String>();
		unique.add("unique1");
		unique.add("unique2");
		uniqueList.put("column1", unique);
		outputTable.uniqueList = uniqueList;
		outputTable.nominalColumn.add("nominalColumn1");
		outputTable.nominalColumn.add("nominalColumn2");
		outputTable.isUnique = true;
		tableMetadata.put("propagatedTable1", outputTable);
		OutputTable outputTable2 = new OutputTable();
		outputTable2.isUnique = true;
		outputTable2.propagatedName = "propagatedName2"; 
		tableMetadata.put("propagatedTable2", outputTable2);
		

	}
	
	@Test
	public void loopPatterns() {
		List<Predictor> list = Aggregation.loopPatterns(setting);
		
		Assert.assertNotNull(list);
		Assert.assertFalse(list.get(0).getPatternCode().isEmpty());		// Will fail if pattern directory is empty
	}
		
	@Test
	public void addTargetValue() {
		ArrayList<String> uniqueList = new ArrayList<String>();
		uniqueList.add("value55");
		List<Predictor> list = Aggregation.addTargetValue(setting, predictor, uniqueList);

		Assert.assertTrue(list.get(0).getParameterMap().containsKey("@targetValue"));
		Assert.assertTrue(list.get(0).getParameterMap().containsValue("value55"));
		Assert.assertFalse(list.get(0).getParameterMap().containsKey("unexpectedKey"));
		Assert.assertFalse(list.get(0).getParameterMap().containsValue("unexpectedValue"));
	}

	@Test
	public void loopParameters() {
		List<Predictor> list = Aggregation.loopParameters(predictor);
		
		Assert.assertEquals(4, list.size());
		Assert.assertTrue(list.get(0).getParameterMap().containsKey("parameterName1"));
		Assert.assertTrue(list.get(0).getParameterMap().containsValue("parameterValue1"));
		Assert.assertTrue(list.get(0).getParameterMap().containsValue("parameterValueA"));
		Assert.assertTrue(list.get(2).getParameterMap().containsValue("parameterValue2"));
		Assert.assertTrue(list.get(2).getParameterMap().containsValue("parameterValueA"));
	}
	
	@Test
	public void loopTables() {
		System.out.println("");
		List<Predictor> list = Aggregation.loopTables(predictor, tableMetadata);
		
		Assert.assertEquals(2, list.size());
		Assert.assertEquals("propagatedName1", list.get(0).propagatedTable);
		Assert.assertEquals("propagatedName2", list.get(1).propagatedTable);
	}
	
	@Test
	public void loopColumns() {
		List<Predictor> list = Aggregation.loopColumns(predictor, tableMetadata);
		
		Assert.assertEquals(2, list.size());
		Assert.assertEquals("nominalColumn1", list.get(0).columnMap.get("@nominalColumn"));
		Assert.assertEquals("nominalColumn2", list.get(1).columnMap.get("@nominalColumn"));
	}
	
	@Test
	public void addSQL() {
		Predictor result = Aggregation.addSQL(predictor, predictor.getPatternCode());
		
		Assert.assertEquals("select @nominalColumn from t1 where col1 = 'value1' and col2 = '@targetValue'", result.getSql());
	}
	
	@Test
	public void addValue() {
		List<Predictor> list = Aggregation.addValue(setting, predictor, tableMetadata);
		
		Assert.assertEquals(2, list.size());
		Assert.assertEquals("unique1", list.get(0).getParameterMap().get("@value"));
		Assert.assertEquals("unique2", list.get(1).getParameterMap().get("@value"));
	}
}
