package correlation;

import java.sql.Connection;
import java.sql.SQLException;

import weka.classifiers.Classifier;

public class DMICorrelationEstimator extends CorrelationEstimator {

	public DMICorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "dmi2";
		makeSQL = new DMIMakeARFFfromSQL(false, true);
	}

	public DMICorrelationEstimator(Connection conn, boolean one80Mode) throws SQLException {
		super(conn);
		function = "dmi2";
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}
