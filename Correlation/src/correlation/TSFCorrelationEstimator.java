package correlation;

import java.sql.Connection;
import java.sql.SQLException;

public class TSFCorrelationEstimator extends CorrelationEstimator {

	public TSFCorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "tsf";
		makeSQL = new TSFMakeARFFfromSQL(false, thirtyDayMode);
	}

}
