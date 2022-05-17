package correlation.Estimators;

import java.sql.Connection;
import java.sql.SQLException;

import correlation.ARFFMaker.TSFMakeARFFfromSQL;

public class TSFCorrelationEstimator extends CorrelationEstimator {

	public TSFCorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "tsf";
		makeSQL = new TSFMakeARFFfromSQL(false, thirtyDayMode);
	}

}
