package correlation;

import java.sql.Connection;
import java.sql.SQLException;

import com.tictactec.ta.lib.Core;

import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class MACDCorrelationEstimatorPhase2 extends CorrelationEstimatorPhase2 {

	Core core = new Core();
	MACDMakeARFFfromSQLPhase2 makeSQL = new MACDMakeARFFfromSQLPhase2(false, !one80Mode);

	public MACDCorrelationEstimatorPhase2(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol, toCloseDays, functionSymbol, fastPeriod, slowPeriod, signalPeriod, functionDaysDiff, doubleBack, correlation, significantPlace  from macd_correlation"
				+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantPlace");
		function = "macd";
	}

	public MACDCorrelationEstimatorPhase2(Connection conn, boolean one80Mode) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol, toCloseDays, functionSymbol, fastPeriod, slowPeriod, signalPeriod, functionDaysDiff, doubleBack, correlation, significantPlace  from macd_correlation_180"
				+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantPlace");
		function = "macd";
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

	@Override
	public double drun(Instances instances) throws Exception {

		IBk classifier = new IBk();
		thisClassifier = classifier;
		classifier.buildClassifier(instances);
		return classifier.classifyInstance(instances.get(instances.size() - 1));
	}

	@Override
	public String buildInstances() {

		try {
			String ret$ = makeSQL.makeARFFFromSQL(sym, daysOut + "", conn, true);
//			PrintWriter pw = new PrintWriter("macdTemp.arff");
//			pw.print(ret$);
//			pw.flush();
//			pw.close();
//			System.exit(0);
			return ret$;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
