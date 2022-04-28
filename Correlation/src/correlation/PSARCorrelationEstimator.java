package correlation;

import java.sql.Connection;
import java.sql.SQLException;

public class PSARCorrelationEstimator extends CorrelationEstimator {

	public PSARCorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "psar";
		makeSQL = new PSARMakeARFFfromSQL(false, thirtyDayMode);
	}

}
