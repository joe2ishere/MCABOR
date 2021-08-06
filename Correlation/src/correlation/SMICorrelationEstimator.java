package correlation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.MAType;

import StochasticMomentum.StochasticMomentum;
import bands.DeltaBands;
import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class SMICorrelationEstimator extends CorrelationEstimator {

	public SMICorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select " + "symbol,   toCloseDays, significantPlace,"
				+ " functionSymbol, hiLowPeriod, maPeriod, smSmoothPeriod, smSignalPeriod, functionDaysDiff, doubleBack,"
				+ " correlation  from sm_correlation"
				+ " where symbol = ? and toCloseDays = ?   order by symbol, toCloseDays, significantplace");
		function = "smi";
	}

	@Override
	public void buildHeaders(PrintWriter pw) throws Exception {

		ResultSet rsSelect = psSelect.executeQuery();
		myParms = new TreeMap<String, Object>();
		nextFunctionSymbol: while (rsSelect.next()) {

			String functionSymbol = rsSelect.getString("functionSymbol");

			GetETFDataUsingSQL pgsd;
			synchronized (gsds) {
				pgsd = gsds.get(functionSymbol);
				if (pgsd == null) {
					// pgsd = GetETFDataUsingSQL.getInstance(functionSymbol);
					if (pgsd == null) {
						System.err.println(
								" *** " + this.function + " is missing data from function symbol =>" + functionSymbol);
						continue nextFunctionSymbol;
					}
					gsds.put(functionSymbol, pgsd);
				}

			}

			if (pgsd.inDate[pgsd.inDate.length - 1].compareTo(currentMktDate) != 0) {
				System.out.println("missing data for " + functionSymbol);
				continue;
			}
			if (startDate.compareTo(pgsd.inDate[50]) < 0)
				startDate = pgsd.inDate[50];

			// hiLowPeriod, maPeriod, smSmoothPeriod, smSignalPeriod,
			int hiLowPeriod = rsSelect.getInt("hiLowPeriod");
			int maPeriod = rsSelect.getInt("maPeriod");
			int smSmoothPeriod = rsSelect.getInt("smSmoothPeriod");
			int smSignalPeriod = rsSelect.getInt("smSignalPeriod");

			StochasticMomentum sm = new StochasticMomentum(pgsd.inHigh, pgsd.inLow, pgsd.inClose, hiLowPeriod,
					MAType.Ema, maPeriod, smSmoothPeriod, smSignalPeriod);
			String symKey = function + "_" + functionSymbol + "_" + rsSelect.getInt("significantPlace");
			myParms.put(symKey, sm);
			functionDaysDiffMap.put(symKey, rsSelect.getInt("functionDaysDiff"));
			doubleBacks.put(symKey, rsSelect.getInt("doubleBack"));
			pw.println("@ATTRIBUTE " + symKey + "smi NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			dates.put(symKey, pgsd.inDate);
		}

	}

	@Override
	public void printAttributeData(int iday, int daysOutCalc, PrintWriter pw,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] inClose, DeltaBands priceBands) {
		SMMakeARFFfromSQL.printAttributeData(iday, daysOutCalc, pw, myParms, functionDaysDiffMap, doubleBacks, arraypos,
				inClose, priceBands, false);

	}

	@Override
	protected void getAttributeText(PrintWriter pw, Object myParm, int functionDaysDiff, int start, int doubleBack) {
		pw.print(SMMakeARFFfromCorrelationFile.getAttributeText((StochasticMomentum) myParm, functionDaysDiff, start,
				doubleBack));

	}

	@Override
	public double drun(Instances instances) throws Exception {
		Classifier classifier = new IBk();
		String args[] = { "-I" };
		((IBk) classifier).setOptions(args);

		classifier.buildClassifier(instances);
		return classifier.classifyInstance(instances.get(instances.size() - 1));
	}

}