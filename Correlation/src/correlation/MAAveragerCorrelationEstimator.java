package correlation;

import java.sql.Connection;
import java.sql.SQLException;

import weka.classifiers.Classifier;

public class MAAveragerCorrelationEstimator extends CorrelationEstimator {

	public MAAveragerCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "maAvg2";
		makeSQL = new MAAveragesMakeARFFfromSQL(false);
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}
