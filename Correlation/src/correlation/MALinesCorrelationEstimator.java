package correlation;

import java.sql.Connection;
import java.sql.SQLException;

import weka.classifiers.Classifier;

public class MALinesCorrelationEstimator extends CorrelationEstimator {

	public MALinesCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "mali";
		makeSQL = new MALinesMakeARFFfromSQL(false);

	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}
