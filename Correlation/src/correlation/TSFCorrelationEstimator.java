package correlation;

import java.sql.Connection;
import java.sql.SQLException;

public class TSFCorrelationEstimator extends CorrelationEstimator {

	public TSFCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "tsf2";
		makeSQL = new TSFMakeARFFfromSQL(false, true);
	}

	public TSFCorrelationEstimator(Connection conn, boolean one80Mode) throws SQLException {
		super(conn);
		function = "tsf2";
	}

}
