package correlation.Estimators.Runners;

import java.sql.Connection;
import java.sql.SQLException;

import correlation.ARFFMaker.DMIMakeARFFfromSQL;
import correlation.ARFFMaker.MAMakeARFFfromSQL;
import weka.classifiers.Classifier;

public class DMICorrelationEstimator extends CorrelationEstimatorRunner {

	public DMICorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "dmi";
		makeSQL = new MAMakeARFFfromSQL(false, true);
	}

	public DMICorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "dmi";
		makeSQL = new DMIMakeARFFfromSQL(false, thirtyDayMode);
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}
