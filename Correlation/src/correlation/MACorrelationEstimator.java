package correlation;

import java.sql.Connection;
import java.sql.SQLException;

import weka.classifiers.Classifier;

public class MACorrelationEstimator extends CorrelationEstimator {

	public MACorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "ma";
		makeSQL = new MAMakeARFFfromSQL(false, true);
	}

	public MACorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "ma";
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}