package correlation.Estimators.Runners;

import java.sql.Connection;
import java.sql.SQLException;

import correlation.ARFFMaker.PSARMakeARFFfromSQL;

public class PSARCorrelationEstimator extends CorrelationEstimatorRunner {

	public PSARCorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "psar";
		makeSQL = new PSARMakeARFFfromSQL(false, thirtyDayMode);
	}

}
