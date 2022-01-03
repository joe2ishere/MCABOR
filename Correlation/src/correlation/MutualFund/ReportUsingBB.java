package correlation.MutualFund;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MAType;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import correlation.BollBandMakeARFFfromSQL;
import util.Averager;
import util.Realign;
import util.getDatabaseConnection;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.Classifier;
import weka.core.Instances;

public class ReportUsingBB {

	public static void main(String[] args) throws Exception {

		String startDate;
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		PreparedStatement psMFGet = conn
				.prepareStatement("select dt, close from fidelitymutualfunds where name = ? order by dt");
		FileReader bbFR = new FileReader("bbCorrelationBBoverSignal_30days_MutualFunds.csv");
		BufferedReader bbBR = new BufferedReader(bbFR);

		String inBBLine = "";

		int daysOut = 30;
		int doubleBack = -1;
		bbBR.readLine(); // skip header

		Averager avg = new Averager();
		String lastSym = "";
		// PriceBands priceBands = null;
		DeltaBands priceBands = null;
		BollBandMakeARFFfromSQL makeFromSQL = new BollBandMakeARFFfromSQL();
		while ((inBBLine = bbBR.readLine()) != null) {
			String inBBData[] = inBBLine.split("_");

			String sym = inBBData[0];
			if (sym.compareTo(lastSym) != 0) {
				if (avg.getCount() > 0)
					System.out.println(avg.get());
				avg = new Averager();
				lastSym = sym;
				System.out.print(sym + ";");
			}

			psMFGet.setString(1, sym);
			daysOut = Integer.parseInt(inBBData[1]);

			ArrayList<Double> closes = new ArrayList<>();
			ArrayList<String> dates = new ArrayList<>();

			ResultSet rsMFGet = psMFGet.executeQuery();
			while (rsMFGet.next()) {
				closes.add((double) rsMFGet.getFloat(2));
				dates.add(rsMFGet.getString(1));

			}

			double dcloses[] = closes.stream().mapToDouble(dbl -> dbl).toArray();

			priceBands = new DeltaBands(dcloses, daysOut, 5);

			startDate = dates.get(50);

			TreeMap<String, Object> bbs = new TreeMap<>();
			TreeMap<String, Integer> bbDaysDiff = new TreeMap<>();
			TreeMap<String, Integer> bbDoubleBacks = new TreeMap<>();

			TreeMap<String, String[]> bbDates = new TreeMap<>();
			StringWriter bbSW = new StringWriter();
			PrintWriter bbPW = new PrintWriter(bbSW);
			bbPW.println("% 1. Title: " + sym + "_bb_correlation");
			bbPW.println("@RELATION " + sym);

			for (int iread = 0; iread < 10; iread++) {
				inBBLine = bbBR.readLine();
				// pos;abscorr;mfSym;priceDaysDiff;bbSym;optInTimePeriod;optInNbDevUp;smDaysDiff;doubleBack;corr;bbSym
				if (inBBLine.length() < 1)
					break;
				inBBData = inBBLine.split(";");
				if (inBBData.length < 2)
					break;
				String symbol = inBBData[4];
				if (inBBData[2].contains(sym) == false)
					continue;

				String symKey = inBBData[4] + "_" + inBBData[3];
				if (bbs.containsKey(symKey))
					continue;
				GetETFDataUsingSQL pgsd;
				try {
					pgsd = GetETFDataUsingSQL.getInstance(symbol);
				} catch (Exception e) {
					// System.out.println(e.getMessage());
					continue;
				}

				if (startDate.compareTo(pgsd.inDate[50]) < 0)
					startDate = pgsd.inDate[50];

				// System.out.print(" " + symbol);
				int optInTimePeriod = Integer.parseInt(inBBData[5]);
				double optInNbDevUp = Double.parseDouble(inBBData[6]);
				int daysDiff = Integer.parseInt(inBBData[3]);
				doubleBack = Integer.parseInt(inBBData[8]);
				if (daysDiff != daysOut) {
					throw new Exception("days diff and out different");
				}
				Core core = new Core();
				MInteger outBegIdx = new MInteger();
				MInteger outNBElement = new MInteger();
				double[] outRealUpperBand = new double[pgsd.inClose.length];
				double[] outRealMiddleBand = new double[pgsd.inClose.length];
				double[] outRealLowerBand = new double[pgsd.inClose.length];
				core.bbands(0, pgsd.inClose.length - 1, pgsd.inClose, optInTimePeriod, optInNbDevUp, optInNbDevUp,
						MAType.Ema, outBegIdx, outNBElement, outRealUpperBand, outRealMiddleBand, outRealLowerBand);
				Realign.realign(outRealUpperBand, outBegIdx);
				Realign.realign(outRealMiddleBand, outBegIdx);
				Realign.realign(outRealLowerBand, outBegIdx);
				ArrayList<double[]> bbands = new ArrayList<>();
				bbands.add(outRealUpperBand);
				bbands.add(outRealMiddleBand);
				bbands.add(outRealLowerBand);

				bbs.put(symKey, bbands);
				bbDaysDiff.put(symKey, daysDiff);
				bbDoubleBacks.put(symKey, doubleBack);
				bbPW.println("@ATTRIBUTE " + symKey + "bb NUMERIC");
				bbPW.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
				bbDates.put(symKey, pgsd.inDate);
			}
			bbBR.readLine();
			{
				bbPW.println(priceBands.getAttributeDefinition());
				bbPW.println("@DATA");
				bbPW.flush();
				StringWriter swtest = new StringWriter();
				PrintWriter pwtest = new PrintWriter(swtest);
				pwtest.print(bbSW.toString());

				int arraypos[] = new int[bbs.size()];
				int pos = 50;
				while (dates.get(pos).compareTo(startDate) < 0)
					pos++;

				eemindexLoop: for (int iday = pos; iday < closes.size() - daysOut - 1;) {

					String edate = dates.get(iday);
					pos = 0;
					for (String key : bbs.keySet()) {
						String sdate = bbDates.get(key)[arraypos[pos]];
						int dcomp = edate.compareTo(sdate);
						if (dcomp < 0) {
							iday++;
							continue eemindexLoop;
						}
						if (dcomp > 0) {
							arraypos[pos]++;
							continue eemindexLoop;
						}
						pos++;
					}

//					makeFromSQL.printAttributeData(iday, daysOut, bbPW, bbs, bbDaysDiff, bbDoubleBacks, arraypos,
//							dcloses, priceBands, false);

					pos = 0;
					for (pos = 0; pos < arraypos.length; pos++) {
						arraypos[pos]++;
					}

				}

				Instances instances = new Instances(new StringReader(bbSW.toString()));
//				PrintWriter pw = new PrintWriter("c:/users/joe/correlationARFF/" + sym + "mfbb.arff");
//				pw.println(bbSW.toString());
//				pw.flush();
//				pw.close();
				instances.setClassIndex(instances.numAttributes() - 1);
				// RandomForest classifier = new RandomForest();
				Classifier classifier = AbstractClassifier.forName("weka.classifiers.lazy.IBk",
						new String[] { "-K", "6", "-X", "-I" });
				classifier.buildClassifier(instances);

				pwtest.flush();

				for (int iday = 1; iday <= 5; iday++) {

					pos = 0;
					for (String key : bbs.keySet()) {
						ArrayList<double[]> bb = (ArrayList<double[]>) bbs.get(key);
						int bbdaysdiff = bbDaysDiff.get(key);
						int bbstart = bbDates.get(key).length - iday;
//						if (pos == 0)
//							System.out.println("\n" + daysOut + " using " + bbDates.get(key)[bbstart] + "; for ;"
//									+ (daysOut - (iday - 1)));
						;
						pwtest.print(makeFromSQL.getAttributeText(bb, bbdaysdiff, bbstart, doubleBack) + ",");
						arraypos[pos]++;
						pos++;

					}

					pwtest.println("?");
					pwtest.flush();
				}
				Instances instancesTest = new Instances(new StringReader(swtest.toString()));
//				System.out.println(swtest.toString());
				instancesTest.setClassIndex(instancesTest.numAttributes() - 1);
				for (int i = 0; i < instancesTest.size(); i++) {
					double got = classifier.classifyInstance(instancesTest.get(i));

					avg.add(priceBands.relativeTo9(got));

				}

			}
		}
		System.out.println(avg.get());
	}

}
