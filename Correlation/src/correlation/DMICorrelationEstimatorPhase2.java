package correlation;

import java.sql.Connection;
import java.sql.SQLException;

import com.tictactec.ta.lib.Core;

import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.classifiers.trees.RandomForest;
import weka.core.EuclideanDistance;
import weka.core.Instances;
import weka.core.neighboursearch.LinearNNSearch;

public class DMICorrelationEstimatorPhase2 extends CorrelationEstimatorPhase2 {

	Core core = new Core();
	DMIMakeARFFfromSQLPhase2 makeSQL = new DMIMakeARFFfromSQLPhase2(false, !one80Mode);

	public DMICorrelationEstimatorPhase2(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol, toCloseDays, significantPlace, functionSymbol, dmiPeriod, functionDaysDiff, correlation  from dmi_correlation"
				+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantplace");
		function = "dmi";
	}

	public DMICorrelationEstimatorPhase2(Connection conn, boolean one80Mode) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol, toCloseDays, significantPlace, functionSymbol, dmiPeriod, functionDaysDiff, correlation  from dmi_correlation_180"
				+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantplace");
		function = "dmi";
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

	@Override
	public double drun(Instances instances) throws Exception {
		IBk classifier = new IBk();
		thisClassifier = classifier;
		/*
		 * classifier.setKNN(2); classifier.setCrossValidate(true);
		 * classifier.setMeanSquared(true); classifier.buildClassifier(instances);
		 * 
		 */

		classifier.buildClassifier(instances);
		return classifier.classifyInstance(instances.get(instances.size() - 1));

	}

	@Override
	public double getBestClassifier(Instances instances) throws Exception {

		if (daysOut == 5) {
			RandomForest classifier = new RandomForest();
			classifier.setOptions(
					new String[] { "-K", "0", "-M", "4.0", "-V", "0.001", "-S", "1", "-do-not-check-capabilities" });
			classifier.buildClassifier(instances);
			return classifier.classifyInstance(instances.get(instances.size() - 1));

		}
		if (daysOut == 4) {
			RandomForest classifier = new RandomForest();
			classifier.setOptions(new String[] { "-K", "16", "-M", "2.0", "-V", "10.0", "-S", "1", "-depth", "9", "-B",
					"-do-not-check-capabilities" });
			classifier.buildClassifier(instances);
			return classifier.classifyInstance(instances.get(instances.size() - 1));
		}
//[-K, 43, -W, 0, -X, -A, weka.core.neighboursearch.LinearNNSearch -A "weka.core.EuclideanDistance -R first-last", -do-not-check-capabilities]]
		IBk classifier = new IBk();
		classifier.setOptions(new String[] { "-K", "43", "-W", "0", "-X", "-do-not-check-capabilities" });
		LinearNNSearch lnns = new LinearNNSearch();
		EuclideanDistance df = new EuclideanDistance();
		df.setAttributeIndices("first-last");
		lnns.setDistanceFunction(df);
		classifier.setNearestNeighbourSearchAlgorithm(lnns);
		classifier.buildClassifier(instances);
		return classifier.classifyInstance(instances.get(instances.size() - 1));

	}

	@Override
	public String buildInstances() {
		try {
			return makeSQL.makeARFFFromSQL(sym, daysOut + "", conn, true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
}
