package correlation;

import java.sql.Connection;
import java.sql.SQLException;

import com.tictactec.ta.lib.Core;

import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class SMICorrelationEstimatorPhase2 extends CorrelationEstimatorPhase2 {

	Core core = new Core();
	SMMakeARFFfromSQLPhase2 makeSQL = new SMMakeARFFfromSQLPhase2(false, !one80Mode);

	public SMICorrelationEstimatorPhase2(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol, toCloseDays, significantPlace, functionSymbol, hiLowPeriod, maPeriod, smSmoothPeriod, smSignalPeriod,"
				+ " functionDaysDiff, correlation  from sm_correlation"
				+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantplace");
		function = "smi";
	}

	public SMICorrelationEstimatorPhase2(Connection conn, boolean one80Mode) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol, toCloseDays, significantPlace, functionSymbol, hiLowPeriod, maPeriod, smSmoothPeriod, smSignalPeriod,"
				+ " functionDaysDiff, correlation  from sm_correlation_180"
				+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantplace");
		function = "smi";
	}

	@Override
	public double drun(Instances instances) throws Exception {

		Classifier classifier = new IBk();

		classifier.buildClassifier(instances);
		return classifier.classifyInstance(instances.get(instances.size() - 1));

	}

	@Override
	public String buildInstances() {

		try {
			return makeSQL.makeARFFFromSQL(sym, daysOut + "", conn, true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
