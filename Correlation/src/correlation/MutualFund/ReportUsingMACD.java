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
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import correlation.MACDMakeARFFfromSQL;
import util.Averager;
import util.Realign;
import util.getDatabaseConnection;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.Classifier;
import weka.core.Instances;

public class ReportUsingMACD {

	public static void main(String[] args) throws Exception {

		String startDate;
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		PreparedStatement psMFGet = conn
				.prepareStatement("select dt, close from fidelitymutualfunds where name = ? order by dt");
		FileReader macdFR = new FileReader("macdCorrelationMACDoverSignal_30days_MutualFunds.csv");
		BufferedReader macdBR = new BufferedReader(macdFR);

		String inMACDLine = "";

		int daysOut = 30;
		int doubleBack = -1;
		macdBR.readLine(); // skip header

		Averager avg = new Averager();
		String lastSym = "";
		// PriceBands priceBands = null;
		DeltaBands priceBands = null;
		MACDMakeARFFfromSQL makeFromSQL = new MACDMakeARFFfromSQL(false);
		while ((inMACDLine = macdBR.readLine()) != null) {
			String inMACDData[] = inMACDLine.split("_");

			String sym = inMACDData[0];
			if (sym.compareTo(lastSym) != 0) {
				if (avg.getCount() > 0)
					System.out.println(avg.get());
				avg = new Averager();
				lastSym = sym;
				System.out.print(sym + ";");
			}

			psMFGet.setString(1, sym);
			daysOut = Integer.parseInt(inMACDData[1]);

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

			TreeMap<String, Object> macds = new TreeMap<>();
			TreeMap<String, Integer> macdDaysDiff = new TreeMap<>();
			TreeMap<String, Integer> macdDoubleBacks = new TreeMap<>();

			TreeMap<String, String[]> macdDates = new TreeMap<>();
			StringWriter macdSW = new StringWriter();
			PrintWriter macdPW = new PrintWriter(macdSW);
			macdPW.println("% 1. Title: " + sym + "_macd_correlation");
			macdPW.println("@RELATION " + sym);

			for (int iread = 0; iread < 10; iread++) {
				inMACDLine = macdBR.readLine();
				// 0;ust;FRESX;16;15;2;35;3;4;0.2406;0.24
				if (inMACDLine.length() < 1)
					break;
				inMACDData = inMACDLine.split(";");
				if (inMACDData.length < 2)
					break;
				String symbol = inMACDData[1];
				if (inMACDData[2].contains(sym) == false)
					continue;

				String symKey = inMACDData[1] + "_" + inMACDData[0];
				if (macds.containsKey(symKey))
					continue;
				GetETFDataUsingSQL pgsd;
				try {
					pgsd = GetETFDataUsingSQL.getInstance(symbol);
				} catch (Exception e) {
					return;
				}

				if (startDate.compareTo(pgsd.inDate[50]) < 0)
					startDate = pgsd.inDate[50];

				// System.out.print(" " + symbol);
				int optInFastPeriod = Integer.parseInt(inMACDData[3]);
				int optInSlowPeriod = Integer.parseInt(inMACDData[4]);
				int optInSignalPeriod = Integer.parseInt(inMACDData[5]);

				int daysDiff = Integer.parseInt(inMACDData[3]);
				doubleBack = Integer.parseInt(inMACDData[8]);
//				if (daysDiff != daysOut) {
//					throw new Exception("days diff and out different");
//				}
				Core core = new Core();
				MInteger outBegIdx = new MInteger();
				MInteger outNBElement = new MInteger();
				double outMACD[] = new double[pgsd.inClose.length];
				double[] outMACDSignal = new double[pgsd.inClose.length];
				double[] outMACDHist = new double[pgsd.inClose.length];

				core.macd(0, pgsd.inClose.length - 1, pgsd.inClose, optInFastPeriod, optInSlowPeriod, optInSignalPeriod,
						outBegIdx, outNBElement, outMACD, outMACDSignal, outMACDHist);

				Realign.realign(outMACD, outBegIdx);
				Realign.realign(outMACDSignal, outBegIdx);
				Realign.realign(outMACDHist, outBegIdx);
				ArrayList<double[]> macdands = new ArrayList<>();
				macdands.add(outMACD);
				macdands.add(outMACDSignal);
				macdands.add(outMACDHist);

				macds.put(symKey, macdands);
				macdDaysDiff.put(symKey, daysDiff);
				macdDoubleBacks.put(symKey, doubleBack);
				macdPW.println("@ATTRIBUTE " + symKey + "macd NUMERIC");
				macdPW.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
				macdDates.put(symKey, pgsd.inDate);
			}
			macdBR.readLine();
			{
				macdPW.println(priceBands.getAttributeDefinition());
				macdPW.println("@DATA");
				macdPW.flush();
				StringWriter swtest = new StringWriter();
				PrintWriter pwtest = new PrintWriter(swtest);
				pwtest.print(macdSW.toString());

				int arraypos[] = new int[macds.size()];
				int pos = 50;
				while (dates.get(pos).compareTo(startDate) < 0)
					pos++;

				eemindexLoop: for (int iday = pos; iday < closes.size() - daysOut - 1;) {

					String edate = dates.get(iday);
					pos = 0;
					for (String key : macds.keySet()) {
						String sdate = macdDates.get(key)[arraypos[pos]];
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

					makeFromSQL.printAttributeData(iday, daysOut, macdPW, macds, macdDaysDiff, macdDoubleBacks,
							arraypos, dcloses, priceBands, false);

					pos = 0;
					for (pos = 0; pos < arraypos.length; pos++) {
						arraypos[pos]++;
					}

				}

				Instances instances = new Instances(new StringReader(macdSW.toString()));
//				PrintWriter pw = new PrintWriter("c:/users/joe/correlationARFF/" + sym + "mfmacd.arff");
//				pw.println(macdSW.toString());
//				pw.flush();
//				pw.close();
				instances.setClassIndex(instances.numAttributes() - 1);
				// RandomForest classifier = new RandomForest();
				Classifier classifier = AbstractClassifier.forName("weka.classifiers.lazy.IBk",
						new String[] { "-K", "2", "-X", "-I" });
				classifier.buildClassifier(instances);

				pwtest.flush();

				for (int iday = 1; iday <= 5; iday++) {

					pos = 0;
					for (String key : macds.keySet()) {
						ArrayList<double[]> macd = (ArrayList<double[]>) macds.get(key);
						int macddaysdiff = macdDaysDiff.get(key);
						int macdstart = macdDates.get(key).length - iday;
//						if (pos == 0)
//							System.out.println("\n" + daysOut + " using " + macdDates.get(key)[macdstart] + "; for ;"
//									+ (daysOut - (iday - 1)));
						;
						pwtest.print(makeFromSQL.getAttributeText(macd, macddaysdiff, macdstart, doubleBack) + ",");
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
