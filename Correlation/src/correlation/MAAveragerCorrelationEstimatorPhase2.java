package correlation;

import java.sql.Connection;
import java.sql.SQLException;

import com.tictactec.ta.lib.Core;

import weka.classifiers.Classifier;
import weka.classifiers.lazy.KStar;
import weka.core.Instances;

public class MAAveragerCorrelationEstimatorPhase2 extends CorrelationEstimatorPhase2 {

	Core core = new Core();
	MAAveragesMakeARFFfromSQLPhase2 makeSQL = new MAAveragesMakeARFFfromSQLPhase2(false);

	public MAAveragerCorrelationEstimatorPhase2(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement(
				"select " + "symbol,   toCloseDays, significantPlace, functionSymbol, period, matype, correlation"
						+ "  from  maline_correlation"
						+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantplace");
		function = "maAvg";
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

	@Override
	public double drun(Instances instances) throws Exception {
		Classifier classifier = new KStar();
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
