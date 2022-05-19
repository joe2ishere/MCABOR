package correlation.Estimators.Runners;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.TreeMap;

import bands.DeltaBands;
import correlation.ARFFMaker.MACDMakeARFFfromSQL;
import correlation.ARFFMaker.SMIandMACDMakeARFFfromSQL;
import correlation.ARFFMaker.SMMakeARFFfromSQL;
import correlation.ARFFMaker.Parms.AttributeParm;
import utils.StandardDeviation;
import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class SMIandMACDCorrelationEstimator extends CorrelationEstimatorRunner {

	SMMakeARFFfromSQL smi_makeSQL;
	MACDMakeARFFfromSQL macd_makeSQL;

	public SMIandMACDCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "smi_macd";
		makeSQL = new SMIandMACDMakeARFFfromSQL(false, true);
		smi_makeSQL = new SMMakeARFFfromSQL(false, true);
		macd_makeSQL = new MACDMakeARFFfromSQL(false, true);
	}

	public SMIandMACDCorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "smi_macd";
	}

	@Override
	public double prun(String sym, int daysOut, DeltaBands priceBands,
			TreeMap<Integer, StandardDeviation> avgForDaysOut2, TreeMap<String, Double> theBadness, boolean whichHalf)
			throws Exception {
		AttributeParm smiParms = null;
		AttributeParm macdParms = null;

		try {
			smiParms = smi_makeSQL.buildParameters(sym, daysOut, conn);
			macdParms = macd_makeSQL.buildParameters(sym, daysOut, conn);
		} catch (Exception e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		String $instances = null;
		try {

			$instances = makeSQL.makeARFFFromSQL(sym, daysOut, smiParms, macdParms, whichHalf);

		} catch (Exception e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		if ($instances == null) {
			System.out.println("if ($instances == null)");
			System.exit(0);
		}

		String $lastLine = null;
		try {

			$lastLine = makeSQL.makeARFFFromSQLForQuestionMark(sym, daysOut, smiParms, macdParms);

		} catch (

		Exception e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		if ($lastLine == null) {
			System.out.println("if ($lastLine == null)");
			System.exit(0);
		}

		Instances trainInst = null;
		try {
			trainInst = new Instances(new StringReader($instances));
		} catch (Exception e) {
			e.printStackTrace();
		}

		trainInst.setClassIndex(trainInst.numAttributes() - 1);

		Classifier classifier = new IBk();

		classifier.buildClassifier(trainInst);

		Instances testInst;
		testInst = new Instances(new StringReader($lastLine));
		testInst.setClassIndex(testInst.numAttributes() - 1);
		return classifier.classifyInstance(testInst.get(testInst.size() - 1));
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}
