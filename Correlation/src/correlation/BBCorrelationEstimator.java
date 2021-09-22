package correlation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MAType;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import util.Realign;
import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class BBCorrelationEstimator extends CorrelationEstimator {

	Core core = new Core();
	BollBandMakeARFFfromSQL makeSQL = new BollBandMakeARFFfromSQL();

	public BBCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol, significantplace,  toCloseDays, functionSymbol, period, stddev, functionDaysDiff, doubleBack, correlation  from bb_correlation"
				+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantplace");
		function = "bb";
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

	@Override
	public double drun(Instances instances) throws Exception {
		Classifier classifier = new IBk();
		String args[] = { "-I" };
		((IBk) classifier).setOptions(args);

		classifier.buildClassifier(instances);

		return classifier.classifyInstance(instances.get(instances.size() - 1));

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

			int period = rsSelect.getInt("period");
			double stddev = rsSelect.getDouble("stddev");

			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();

			double[] outRealUpperBand = new double[pgsd.inClose.length];
			double[] outRealMiddleBand = new double[pgsd.inClose.length];
			double[] outRealLowerBand = new double[pgsd.inClose.length];
			core.bbands(0, pgsd.inClose.length - 1, pgsd.inClose, period, stddev, stddev, MAType.Ema, outBegIdx,
					outNBElement, outRealUpperBand, outRealMiddleBand, outRealLowerBand);

			Realign.realign(outRealUpperBand, outBegIdx);
			Realign.realign(outRealMiddleBand, outBegIdx);
			Realign.realign(outRealLowerBand, outBegIdx);
			ArrayList<double[]> bbData = new ArrayList<>();
			bbData.add(outRealUpperBand);
			bbData.add(outRealMiddleBand);
			bbData.add(outRealLowerBand);
			String symKey = function + "_" + functionSymbol + "_" + rsSelect.getInt("significantPlace");

			myParms.put(symKey, bbData);

			functionDaysDiffMap.put(symKey, rsSelect.getInt("functionDaysDiff"));
			pw.println("@ATTRIBUTE " + symKey + "bb NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "bbSignal NUMERIC");
			dates.put(symKey, pgsd.inDate);
		}

	}

	@Override
	public void printAttributeData(int iday, int daysOutCalc, PrintWriter pw,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] inClose, DeltaBands priceBands) {
		makeSQL.printAttributeData(iday, daysOutCalc, pw, myParms, functionDaysDiffMap, doubleBacks, arraypos, inClose,
				priceBands, true);

	}

	@Override
	protected void getAttributeText(PrintWriter pw, Object inObject, int functionDaysDiff, int start, int doubleBack) {

		ArrayList<double[]> bbData = (ArrayList<double[]>) inObject;
		pw.print(makeSQL.getAttributeText(bbData, functionDaysDiff, start, doubleBack));

	}

}
