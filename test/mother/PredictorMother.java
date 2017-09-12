package mother;

import extraction.Predictor;
import run.Setting;

import java.time.LocalDateTime;

public abstract class PredictorMother {

	public static final Setting setting = new Setting("MariaDB", "mutagenesis_multiple");
	public static final String mutagenic = setting.baseTargetPrefix + 1;
	public static final String ind1 = setting.baseTargetPrefix + 2;

	// Relevances:
	//  0.5 0   woeMutagenic
	//  0   0.8 woeInd1
	//  0.7 0.2 aggregateMax
	//  0.6 0.1 aggregateMin
	//  0.4 0.9 aggregateAvg

	public static Predictor woeMutagenic() {
		Predictor predictor = new Predictor(PatternMother.woe());

		predictor.setRelevance(mutagenic, 0.5);
		predictor.setBaseTarget(mutagenic);
		predictor.setTargetColumn("mutagenic");
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("woeMutagenic");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_1");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(1);
		predictor.setGroupId(1);

		return predictor;
	}

	public static Predictor woeInd1() {
		Predictor predictor = new Predictor(PatternMother.woe());

		predictor.setRelevance(ind1, 0.8);
		predictor.setBaseTarget(ind1);
		predictor.setTargetColumn("ind1");
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("woeInd1");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_2");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(2);
		predictor.setGroupId(2);

		return predictor;
	}

	public static Predictor aggregateMax() {
		Predictor predictor = new Predictor(PatternMother.aggregate());

		predictor.setRelevance(mutagenic, 0.7);
		predictor.setRelevance(ind1, 0.2);
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("aggregateMax");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_3");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(3);
		predictor.setGroupId(3);

		return predictor;
	}

	public static Predictor aggregateMin() {
		Predictor predictor = new Predictor(PatternMother.aggregate());

		predictor.setRelevance(mutagenic, 0.6);
		predictor.setRelevance(ind1, 0.1);
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("aggregateMin");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_4");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(4);
		predictor.setGroupId(4);

		return predictor;
	}

	public static Predictor aggregateAvg() {
		Predictor predictor = new Predictor(PatternMother.aggregate());

		predictor.setRelevance(mutagenic, 0.4);
		predictor.setRelevance(ind1, 0.9);
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("aggregateAvg");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_5");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(5);
		predictor.setGroupId(5);

		return predictor;
	}

	public static Predictor faulty() {
		Predictor predictor = new Predictor(PatternMother.aggregate());

		predictor.setRelevance(mutagenic, 0.0);
		predictor.setRelevance(ind1, 0.0);
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("faulty");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_6");
		predictor.setSql("select * from table");
		predictor.setOk(false);
		predictor.setId(6);
		predictor.setGroupId(6);

		return predictor;
	}

	public static Predictor aggregateFrame1() {
		Predictor predictor = new Predictor(PatternMother.aggregate());

		predictor.setRelevance(mutagenic, 0.2);
		predictor.setRelevance(ind1, 0.1);
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("frame1");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_7");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(7);
		predictor.setGroupId(7);

		return predictor;
	}

	public static Predictor aggregateFrame2() {
		Predictor predictor = new Predictor(PatternMother.aggregate());

		predictor.setRelevance(mutagenic, 0.1);
		predictor.setRelevance(ind1, 0.3);
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("frame2");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_8");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(8);
		predictor.setGroupId(7);

		return predictor;
	}

	public static Predictor aggregateFrame3() {
		Predictor predictor = new Predictor(PatternMother.aggregate());

		predictor.setRelevance(mutagenic, 0.3);
		predictor.setRelevance(ind1, 0.4);
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("frame3");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_9");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(9);
		predictor.setGroupId(7);

		return predictor;
	}

	public static Predictor woeFrameMutagenic1() {
		Predictor predictor = new Predictor(PatternMother.woe());

		predictor.setRelevance(mutagenic, 0.4);
		predictor.setBaseTarget(mutagenic);
		predictor.setTargetColumn("mutagenic");
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("woe1");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_10");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(10);
		predictor.setGroupId(8);
		predictor.setChosenBaseTarget("mutagenic");

		return predictor;
	}

	public static Predictor woeFrameMutagenic2() {
		Predictor predictor = new Predictor(PatternMother.woe());

		predictor.setRelevance(mutagenic, 0.5);
		predictor.setBaseTarget(mutagenic);
		predictor.setTargetColumn("mutagenic");
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("woe2");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_11");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(11);
		predictor.setGroupId(8);
		predictor.setChosenBaseTarget("mutagenic");

		return predictor;
	}

	public static Predictor aggregateStd() {
		Predictor predictor = new Predictor(PatternMother.aggregate());

		predictor.setRelevance(mutagenic, 0.5);
		predictor.setRelevance(ind1, 0.05);
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("aggregateMax");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_12");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(12);
		predictor.setGroupId(9);

		return predictor;
	}

	public static Predictor zeroRelevance() {
		Predictor predictor = new Predictor(PatternMother.aggregate());

		predictor.setRelevance(mutagenic, 0.0);
		predictor.setRelevance(ind1, 0.0);
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("zeroRelevance");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_13");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(13);
		predictor.setGroupId(10);

		return predictor;
	}

	public static Predictor zeroConceptDrift() {
		Predictor predictor = new Predictor(PatternMother.aggregate());

		predictor.setRelevance(mutagenic, 1.0);
		predictor.setRelevance(ind1, 1.0);
		predictor.setConceptDrift(mutagenic, 0.0);
		predictor.setConceptDrift(ind1, 0.0);
		predictor.setTimestampBuilt(LocalDateTime.now());
		predictor.setTimestampDelivered(LocalDateTime.now());
		predictor.setName("zeroConceptDrift");
		predictor.getTable().originalName = "originalTable";
		predictor.setOutputTable("predictor_14");
		predictor.setSql("select * from table");
		predictor.setOk(true);
		predictor.setId(14);
		predictor.setGroupId(11);

		return predictor;
	}
}
