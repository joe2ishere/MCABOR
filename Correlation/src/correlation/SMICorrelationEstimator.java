package correlation;

import java.sql.Connection;
import java.sql.SQLException;

public class SMICorrelationEstimator extends CorrelationEstimator {

	public SMICorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "smi";
		makeSQL = new SMMakeARFFfromSQL(false, thirtyDayMode);
	}

}
