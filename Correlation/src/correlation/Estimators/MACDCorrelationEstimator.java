package correlation.Estimators;

import java.sql.Connection;
import java.sql.SQLException;

import correlation.ARFFMaker.MACDMakeARFFfromSQL;
import weka.classifiers.Classifier;

public class MACDCorrelationEstimator extends CorrelationEstimator {

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
