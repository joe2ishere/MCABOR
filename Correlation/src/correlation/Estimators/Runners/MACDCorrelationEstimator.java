package correlation.Estimators.Runners;

import java.sql.Connection;
import java.sql.SQLException;

import correlation.ARFFMaker.MACDMakeARFFfromSQL;
import correlation.ARFFMaker.MAMakeARFFfromSQL;
import weka.classifiers.Classifier;

public class MACDCorrelationEstimator extends CorrelationEstimatorRunner {

	public MACDCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "macd";
		makeSQL = new MAMakeARFFfromSQL(false, true);
	}

	public MACDCorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "macd";
		makeSQL = new MACDMakeARFFfromSQL(false, thirtyDayMode);
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}
