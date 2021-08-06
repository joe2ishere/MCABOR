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
import weka.attributeSelection.ASEvaluation;
import weka.attributeSelection.ASSearch;
import weka.attributeSelection.AttributeSelection;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

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

	@Override
	public double drun(Instances instances) throws Exception {

		IBk classifier = new IBk();
		String options[] = { "-K", "2", "-X", "-I" };
		classifier.setOptions(options);
		classifier.setMeanSquared(true);
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
			AttributeSelection as = new AttributeSelection();
			ASSearch asSearch = ASSearch.forName("weka.attributeSelection.GreedyStepwise",
					new String[] { "-C", "-B", "-R" });
			as.setSearch(asSearch);
			ASEvaluation asEval = ASEvaluation.forName("weka.attributeSelection.CfsSubsetEval", new String[] { "-L" });
			as.setEvaluator(asEval);
			as.SelectAttributes(instances);
			Instances instances2 = as.reduceDimensionality(instances);
			Classifier classifier = AbstractClassifier.forName("weka.classifiers.meta.RandomSubSpace",
					new String[] { "-I", "47", "-P", "0.48773063406106953", "-S", "1", "-W",
							"weka.classifiers.lazy.IBk", "--", "-E", "-K", "1", "-X", "-I" });
			classifier.buildClassifier(instances);
			classifier.buildClassifier(instances2);
			return classifier.classifyInstance(instances2.get(instances2.size() - 1));
		}
		if (daysOut == 5) {
			Classifier classifier = AbstractClassifier.forName("weka.classifiers.lazy.IBk",
					new String[] { "-E", "-K", "1", "-X", "-F" });
			classifier.buildClassifier(instances);
			return classifier.classifyInstance(instances.get(instances.size() - 1));

		}
		AttributeSelection as = new AttributeSelection();
		ASSearch asSearch = ASSearch.forName("weka.attributeSelection.GreedyStepwise",
				new String[] { "-C", "-B", "-R" });
		as.setSearch(asSearch);
		ASEvaluation asEval = ASEvaluation.forName("weka.attributeSelection.CfsSubsetEval", new String[] { "-L" });
		as.setEvaluator(asEval);
		as.SelectAttributes(instances);
		Instances instances2 = as.reduceDimensionality(instances);
		Classifier classifier = AbstractClassifier.forName("weka.classifiers.meta.RandomSubSpace",
				new String[] { "-I", "47", "-P", "0.48773063406106953", "-S", "1", "-W", "weka.classifiers.lazy.IBk",
						"--", "-E", "-K", "1", "-X", "-I" });
		classifier.buildClassifier(instances2);
		return classifier.classifyInstance(instances2.get(instances2.size() - 1));
	}
}
