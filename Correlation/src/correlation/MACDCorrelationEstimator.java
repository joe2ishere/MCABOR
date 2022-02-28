package correlation;

import java.sql.Connection;
import java.sql.SQLException;

import weka.classifiers.Classifier;

public class MACDCorrelationEstimator extends CorrelationEstimator {

	public MACDCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "macd2";
		makeSQL = new MACDMakeARFFfromSQL(false, true);

	}

	public MACDCorrelationEstimator(Connection conn, boolean one80Mode) throws SQLException {
		super(conn);
		function = "macd2";
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}
