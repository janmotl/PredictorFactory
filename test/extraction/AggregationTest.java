package extraction;

import meta.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import run.Setting;

import java.util.*;

public class AggregationTest {
	private Setting setting = new Setting("PostgreSQL", "financial");
	private Pattern pattern = new Pattern();
	private Predictor predictor;
	private List<OutputTable> tableMetadata = new ArrayList<>();
	
	@Before
	public void initialize(){

		// Initialize setting
		setting.targetUniqueValueMap = new HashMap<>();
		LinkedHashMap<String, Integer> uniqueMap = new LinkedHashMap<>();
		uniqueMap.put("value55", 1);
		setting.targetUniqueValueMap.put("status", uniqueMap);


		// Initialize pattern
		SortedMap<String, String> parameter = new TreeMap<>();
		parameter.put("parameterName1", "parameterValue1,parameterValue2");
		parameter.put("parameterName2", "parameterValueA,parameterValueB");
		pattern.dialectParameter = parameter;
		pattern.name = "patternName";
		pattern.dialectCode = "select @nominalColumn from t1 where col1 = '@value' and col2 = '@targetValue'";
		pattern.author = "Majestic Tester";
		pattern.cardinality = "1";
		pattern.description = "Once upon a time...";


		// Initialize tableMetadata
		OutputTable outputTable = new OutputTable();
		outputTable.name = "propagatedName1";
		Column column = new Column("column1");
		Column nominalColumn1 = new Column("nominalColumn1");
		nominalColumn1.isNominal = true;
		Column nominalColumn2 = new Column("nominalColumn2");
		nominalColumn2.isNominal = true;
		outputTable.columnMap.put("column1", column);
		outputTable.columnMap.put("nominalColumn1", nominalColumn1);
		outputTable.columnMap.put("nominalColumn2", nominalColumn2);
		outputTable.isTargetIdUnique = true;

		tableMetadata.add(outputTable);
		OutputTable outputTable2 = new OutputTable();
		outputTable2.isTargetIdUnique = true;
		outputTable2.name = "propagatedName2";
		tableMetadata.add(outputTable2);


		// Initialize predictor
		predictor = new Predictor(pattern);
		predictor.setSql("select * from t1 where propagated_target = '@targetValue'");
		predictor.setTable(outputTable);
		predictor.getColumnMap().put("@nominalColumn", "column1");
		predictor.setParameter("@value", "value1");
	}
	
	@Test
	public void loopPatterns() {
		List<Predictor> list = Aggregation.loopPatterns(setting);
		
		Assert.assertNotNull(list);
		Assert.assertFalse(list.get(0).getPatternCode().isEmpty());		// Will fail if pattern directory is empty
	}
		
	@Test
	public void addTargetValue() {
		List<Predictor> list = Aggregation.addTargetValue(setting, predictor);

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
		List<Predictor> list = Aggregation.loopTables(predictor, tableMetadata);
		
		Assert.assertEquals(2, list.size());
		Assert.assertEquals("propagatedName1", list.get(0).getPropagatedTable());
		Assert.assertEquals("propagatedName2", list.get(1).getPropagatedTable());
	}
	
	@Test
	public void loopColumns() {
		List<Predictor> list = Aggregation.loopColumns(setting, predictor);
		
		Assert.assertEquals(2, list.size());
		Assert.assertEquals("nominalColumn1", list.get(0).getColumnMap().get("@nominalColumn"));
		Assert.assertEquals("nominalColumn2", list.get(1).getColumnMap().get("@nominalColumn"));
	}
	
	@Test
	public void addSQL() {
		Aggregation.addSQL(predictor, predictor.getPatternCode());
		
		Assert.assertEquals("select @nominalColumn from t1 where col1 = 'value1' and col2 = '@targetValue'", predictor.getSql());
	}
}
