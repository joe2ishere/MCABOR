package correlation.Estimators.Runners;

import java.sql.Connection;
import java.sql.SQLException;

import correlation.ARFFMaker.TSFMakeARFFfromSQL;

public class TSFCorrelationEstimator extends CorrelationEstimatorRunner {

	public TSFCorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "tsf";
		makeSQL = new TSFMakeARFFfromSQL(false, thirtyDayMode);
	}

}
