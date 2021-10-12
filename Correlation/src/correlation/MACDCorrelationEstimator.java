package correlation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import util.Realign;
import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.EuclideanDistance;
import weka.core.Instances;
import weka.core.neighboursearch.LinearNNSearch;

public class MACDCorrelationEstimator extends CorrelationEstimator {

	Core core = new Core();
	MACDMakeARFFfromSQL makeSQL = new MACDMakeARFFfromSQL(false);

	public MACDCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol, toCloseDays, functionSymbol, fastPeriod, slowPeriod, signalPeriod, functionDaysDiff, doubleBack, correlation, significantPlace  from macd_correlation"
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

			int fastPeriod = rsSelect.getInt("fastPeriod");
			int slowPeriod = rsSelect.getInt("slowPeriod");
			int signalPeriod = rsSelect.getInt("signalPeriod");
			double outMACD[] = new double[pgsd.inClose.length];
			double outMACDSignal[] = new double[pgsd.inClose.length];
			double outMACDHist[] = new double[pgsd.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.macd(0, pgsd.inClose.length - 1, pgsd.inClose, fastPeriod, slowPeriod, signalPeriod, outBegIdx,
					outNBElement, outMACD, outMACDSignal, outMACDHist);
			Realign.realign(outMACD, outBegIdx);
			Realign.realign(outMACDSignal, outBegIdx);
			Realign.realign(outMACDHist, outBegIdx);
			ArrayList<double[]> macdData = new ArrayList<>();
			macdData.add(outMACD);
			macdData.add(outMACDSignal);
			String symKey = function + "_" + functionSymbol + "_" + rsSelect.getInt("significantPlace");

			myParms.put(symKey, macdData);

			functionDaysDiffMap.put(symKey, rsSelect.getInt("functionDaysDiff"));
			pw.println("@ATTRIBUTE " + symKey + "macd NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "macdSignal NUMERIC");
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

		ArrayList<double[]> macdData = (ArrayList<double[]>) inObject;
		pw.print(makeSQL.getAttributeText(macdData, functionDaysDiff, start, doubleBack));

	}

	@Override
	public double getBestClassifier(Instances instances) throws Exception {
		if (daysOut == 4) {
			// wrappedClassifier=weka.classifiers.lazy.IBk- [-K, 30, -W, 0, -X,
			// -A, weka.core.neighboursearch.LinearNNSearch -A "weka.core.EuclideanDistance
			// -R first-last", -do-not-check-capabilities]]
			IBk classifier = new IBk();
			classifier.setOptions(new String[] { "-K", "30", "-W", "0", "-X", "-do-not-check-capabilities" });
			LinearNNSearch lnns = new LinearNNSearch();
			EuclideanDistance df = new EuclideanDistance();
			df.setAttributeIndices("first-last");
			lnns.setDistanceFunction(df);
			classifier.setNearestNeighbourSearchAlgorithm(lnns);
			classifier.buildClassifier(instances);
			return classifier.classifyInstance(instances.get(instances.size() - 1));
		}
		if (daysOut == 5) {
			// wrappedClassifier=weka.classifiers.lazy.IBk- [-K, 59, -W, 0, -X, -E, -I,
			// -A, weka.core.neighboursearch.LinearNNSearch -A "weka.core.EuclideanDistance
			// -R first-last", -do-not-check-capabilities]]
			IBk classifier = new IBk();
			classifier.setOptions(new String[] { "-K", "59", "-W", "0", "-X", "-E", "-do-not-check-capabilities" });
			LinearNNSearch lnns = new LinearNNSearch();
			EuclideanDistance df = new EuclideanDistance();
			df.setAttributeIndices("first-last");
			lnns.setDistanceFunction(df);
			classifier.setNearestNeighbourSearchAlgorithm(lnns);
			classifier.buildClassifier(instances);
			return classifier.classifyInstance(instances.get(instances.size() - 1));
		}
		// weka.classifiers.lazy.IBk- [-K, 56, -W, 0, -X, -A,
		// weka.core.neighboursearch.LinearNNSearch
		// -A "weka.core.EuclideanDistance -R first-last", -do-not-check-capabilities]]
		IBk classifier = new IBk();
		classifier.setOptions(new String[] { "-K", "56", "-W", "0", "-X", "-do-not-check-capabilities" });
		LinearNNSearch lnns = new LinearNNSearch();
		EuclideanDistance df = new EuclideanDistance();
		df.setAttributeIndices("first-last");
		lnns.setDistanceFunction(df);
		classifier.setNearestNeighbourSearchAlgorithm(lnns);
		classifier.buildClassifier(instances);
		return classifier.classifyInstance(instances.get(instances.size() - 1));

	}
}
