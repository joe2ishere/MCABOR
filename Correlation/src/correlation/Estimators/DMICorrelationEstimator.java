package correlation.Estimators;

import java.sql.Connection;
import java.sql.SQLException;

import correlation.ARFFMaker.DMIMakeARFFfromSQL;
import weka.classifiers.Classifier;

public class DMICorrelationEstimator extends CorrelationEstimator {

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
