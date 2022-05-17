package correlation.Estimators;

import java.sql.Connection;
import java.sql.SQLException;

import correlation.ARFFMaker.ATRMakeARFFfromSQL;
import weka.classifiers.Classifier;

public class ATRCorrelationEstimator extends CorrelationEstimator {

	public ATRCorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "atr";
		makeSQL = new ATRMakeARFFfromSQL(false, thirtyDayMode);
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}
