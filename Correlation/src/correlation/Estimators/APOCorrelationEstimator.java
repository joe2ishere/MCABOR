package correlation.Estimators;

import java.sql.Connection;
import java.sql.SQLException;

import correlation.ARFFMaker.APOMakeARFFfromSQL;
import weka.classifiers.Classifier;

public class APOCorrelationEstimator extends CorrelationEstimator {

	public APOCorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "apo";
		makeSQL = new APOMakeARFFfromSQL(false, thirtyDayMode);
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}
