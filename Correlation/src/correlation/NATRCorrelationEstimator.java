package correlation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import util.Realign;
import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class NATRCorrelationEstimator extends CorrelationEstimator {

	Core core = new Core();

	public NATRCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol,   toCloseDays, significantPlace, functionSymbol, natrPeriod, functionDaysDiff, doubleBack, correlation  from natr_correlation"
				+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantplace");
		function = "natr";
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

			int natrPeriod = rsSelect.getInt("natrPeriod");

			double natr[] = new double[pgsd.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.natr(0, pgsd.inClose.length - 1, pgsd.inHigh, pgsd.inLow, pgsd.inClose, natrPeriod, outBegIdx,
					outNBElement, natr);
			Realign.realign(natr, outBegIdx.value);
			String symKey = function + "_" + functionSymbol + "_" + rsSelect.getInt("significantPlace");

			myParms.put(symKey, natr);
			functionDaysDiffMap.put(symKey, rsSelect.getInt("functionDaysDiff"));
			pw.println("@ATTRIBUTE " + symKey + "natr NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			dates.put(symKey, pgsd.inDate);
		}

	}

	@Override
	public void printAttributeData(int iday, int daysOutCalc, PrintWriter pw,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] inClose, DeltaBands priceBands) {
		NATRMakeARFFfromSQL.printAttributeData(iday, daysOutCalc, pw, myParms, functionDaysDiffMap, doubleBacks,
				arraypos, inClose, priceBands);

	}

	@Override
	protected void getAttributeText(PrintWriter pw, Object myParm, int functionDaysDiff, int start, int doubleBack) {

		pw.print(NATRMakeARFFfromSQL.getAttributeText((double[]) myParm, functionDaysDiff, start, doubleBack));

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
