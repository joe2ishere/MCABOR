package correlation;

import java.sql.Connection;
import java.sql.SQLException;

import com.tictactec.ta.lib.Core;

import weka.classifiers.Classifier;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;

public class TSFCorrelationEstimatorPhase2 extends CorrelationEstimatorPhase2 {

	Core core = new Core();
	TSFMakeARFFfromSQLPhase2 makeSQL = new TSFMakeARFFfromSQLPhase2(false, (!one80Mode));

	public TSFCorrelationEstimatorPhase2(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol, toCloseDays, significantPlace, functionSymbol, tsfPeriod, functionDaysDiff, correlation  from tsf_correlation"
				+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantplace");
		function = "tsf";
	}

	public TSFCorrelationEstimatorPhase2(Connection conn, boolean one80Mode) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol, toCloseDays, significantPlace, functionSymbol, tsfPeriod, functionDaysDiff, correlation  from tsf_correlation_180"
				+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantplace");
		function = "tsf";
	}

	@Override
	public double drun(Instances instances) throws Exception {

		Classifier classifier = new RandomForest();
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
