package correlation;

import java.sql.Connection;
import java.sql.SQLException;

import weka.classifiers.Classifier;

public class ATRCorrelationEstimator extends CorrelationEstimator {

	public ATRCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "atr2";
		makeSQL = new ATRMakeARFFfromSQL(false, true);
	}

	public ATRCorrelationEstimator(Connection conn, boolean one80Mode) throws SQLException {
		super(conn);
		function = "atr2";
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}
