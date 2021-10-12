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
import weka.classifiers.trees.RandomForest;
import weka.core.EuclideanDistance;
import weka.core.Instances;
import weka.core.neighboursearch.LinearNNSearch;

public class TSFCorrelationEstimator extends CorrelationEstimator {

	Core core = new Core();
	TSFMakeARFFfromSQL makeSQL = new TSFMakeARFFfromSQL(false);

	public TSFCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol, toCloseDays, significantPlace, functionSymbol, tsfPeriod, functionDaysDiff, correlation  from tsf_correlation"
				+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantplace");
		function = "tsf";
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

	@Override
	public double drun(Instances instances) throws Exception {

		RandomForest classifier = new RandomForest();
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

			int TSFPeriod = rsSelect.getInt("TSFPeriod");

			double TSF[] = new double[pgsd.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.tsf(0, pgsd.inClose.length - 1, pgsd.inClose, TSFPeriod, outBegIdx, outNBElement, TSF);
			Realign.realign(TSF, outBegIdx.value);
			String symKey = function + "_" + functionSymbol + "_" + rsSelect.getInt("significantPlace");
			myParms.put(symKey, TSF);
			functionDaysDiffMap.put(symKey, rsSelect.getInt("functionDaysDiff"));
			pw.println("@ATTRIBUTE " + symKey + "tsf NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "tsfBack NUMERIC");
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
	protected void getAttributeText(PrintWriter pw, Object myParm, int functionDaysDiff, int start, int doubleBack) {

		pw.print(makeSQL.getAttributeText((double[]) myParm, functionDaysDiff, start, doubleBack));

	}

	@Override
	public double getBestClassifier(Instances instances) throws Exception {

		if (daysOut == 4) {
			RandomForest classifier = new RandomForest();
			// -P, 100, -I, 90, -num-slots, 1, -do-not-check-capabilities, -K, 0, -M, 1.0,
			// -V, 0.001, -S, 1
			classifier.setOptions(new String[] { "-P", "100", "-I", "90", "-num-slots", "1", "-K", "0", "-M", "1.0",
					"-V", "0.001", "-S", "1", "-do-not-check-capabilities" });
			classifier.buildClassifier(instances);
			return classifier.classifyInstance(instances.get(instances.size() - 1));
		}
		if (daysOut == 5) {
			// name=weka.classifiers.lazy.IBk, options=[-K, 47, -W, 0, -X, -F, -A,
			// weka.core.neighboursearch.LinearNNSearch -A "weka.core.EuclideanDistance -R
			// first-last", -do-not-check-capabilities]

			IBk classifier = new IBk();
			classifier.setOptions(new String[] { "-K", "47", "-W", "0", "-X", "-F", "-do-not-check-capabilities" });
			LinearNNSearch lnns = new LinearNNSearch();
			EuclideanDistance df = new EuclideanDistance();
			df.setAttributeIndices("first-last");
			lnns.setDistanceFunction(df);
			classifier.setNearestNeighbourSearchAlgorithm(lnns);
			classifier.buildClassifier(instances);
			return classifier.classifyInstance(instances.get(instances.size() - 1));

		}
//-P, 100, -I, 163, -num-slots, 1, -do-not-check-capabilities, -K, 0, 
		// -M, 4.0, -V, 100.0, -S, 1, -depth, 19, -N, 4]
		RandomForest classifier = new RandomForest();

		classifier.setOptions(new String[] { "-P", "100", "-I", "163", "-num-slots", "1", "-K", "0", "-M", "4.0",
				"-depth", "19", "-N", "4", "-V", "100.", "-S", "1", "-do-not-check-capabilities" });
		classifier.buildClassifier(instances);
		return classifier.classifyInstance(instances.get(instances.size() - 1));

	}

}
