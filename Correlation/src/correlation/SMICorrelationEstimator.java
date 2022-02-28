package correlation;

import java.sql.Connection;
import java.sql.SQLException;

public class SMICorrelationEstimator extends CorrelationEstimator {

	public SMICorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "smi2";
		makeSQL = new SMMakeARFFfromSQL(false, true);
	}

	public SMICorrelationEstimator(Connection conn, boolean one80Mode) throws SQLException {
		super(conn);
		function = "smi2";
	}

}
