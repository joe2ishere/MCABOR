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
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;

public class ATRCorrelationEstimator extends CorrelationEstimator {

	Core core = new Core();
	ATRMakeARFFfromSQL makeSQL = new ATRMakeARFFfromSQL(false);

	public ATRCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement("select "
				+ "symbol,  toCloseDays, significantPlace, functionSymbol, period, functionDaysDiff, doubleBack, correlation  from atr_correlation"
				+ " where symbol = ? and toCloseDays = ?   order by symbol, toCloseDays, significantplace");
		function = "atr";
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

			double atr[] = new double[pgsd.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();

			core.atr(0, pgsd.inClose.length - 1, pgsd.inHigh, pgsd.inLow, pgsd.inClose, period, outBegIdx, outNBElement,
					atr);

			Realign.realign(atr, outBegIdx.value);
			String symKey = function + "_" + functionSymbol + "_" + rsSelect.getInt("significantPlace");
			myParms.put(symKey, atr);
			functionDaysDiffMap.put(symKey, rsSelect.getInt("functionDaysDiff"));
			pw.println("@ATTRIBUTE " + symKey + "atr NUMERIC");
			// pw.println("@ATTRIBUTE " + symKey + "atrback NUMERIC");
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
	public double drun(Instances instances) throws Exception {

		AttributeSelection as = new AttributeSelection();
		ASSearch asSearch = ASSearch.forName("weka.attributeSelection.GreedyStepwise", new String[] { "-B", "-R" });
		as.setSearch(asSearch);
		ASEvaluation asEval = ASEvaluation.forName("weka.attributeSelection.CfsSubsetEval", new String[] {});
		as.setEvaluator(asEval);
		as.SelectAttributes(instances);
		Instances instances2 = as.reduceDimensionality(instances);
		IBk classifier = new IBk();
		String options[] = { "-K", "1", "-X", "-I" };
		classifier.setOptions(options);
		classifier.setMeanSquared(true);
		classifier.buildClassifier(instances2);
		return classifier.classifyInstance(instances2.get(instances2.size() - 1));
	}

	@Override
	public double getBestClassifier(Instances instances) throws Exception {

		if (daysOut == 4) {
			AttributeSelection as = new AttributeSelection();
			ASSearch asSearch = ASSearch.forName("weka.attributeSelection.GreedyStepwise", new String[] { "-C", "-R" });
			as.setSearch(asSearch);
			ASEvaluation asEval = ASEvaluation.forName("weka.attributeSelection.CfsSubsetEval", new String[] { "-M" });
			as.setEvaluator(asEval);
			as.SelectAttributes(instances);
			Instances instances2 = as.reduceDimensionality(instances);
			Classifier classifier = AbstractClassifier.forName("weka.classifiers.lazy.KStar",
					new String[] { "-B", "31", "-M", "n" });
			classifier.buildClassifier(instances2);
			classifier.buildClassifier(instances2);
			return classifier.classifyInstance(instances2.get(instances2.size() - 1));
		}
		if (daysOut == 5) {
			AttributeSelection as = new AttributeSelection();
			ASSearch asSearch = ASSearch.forName("weka.attributeSelection.GreedyStepwise", new String[] { "-R" });
			as.setSearch(asSearch);
			ASEvaluation asEval = ASEvaluation.forName("weka.attributeSelection.CfsSubsetEval",
					new String[] { "-M", "-L" });
			as.setEvaluator(asEval);
			as.SelectAttributes(instances);
			Instances instances2 = as.reduceDimensionality(instances);
			Classifier classifier = AbstractClassifier.forName("weka.classifiers.meta.AdditiveRegression",
					new String[] { "-S", "0.2141750262997668", "-I", "20", "-W", "weka.classifiers.lazy.IBk", "--",
							"-K", "7", "-I" });
			classifier.buildClassifier(instances2);
			return classifier.classifyInstance(instances2.get(instances2.size() - 1));
		}
		RandomForest classifier = new RandomForest();
		classifier.setOptions(new String[] { "-I", "136", "-K", "7", "-depth", "14" });
		classifier.buildClassifier(instances);
		return classifier.classifyInstance(instances.get(instances.size() - 1));
	}
}
