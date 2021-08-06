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
import correlation.TSFMakeARFFfromSQL;
import util.Averager;
import util.getDatabaseConnection;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class MakeMFBuySellReportUsingTSF {

	public static void main(String[] args) throws Exception {

		String startDate;
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		PreparedStatement psMFGet = conn
				.prepareStatement("select dt, close from fidelitymutualfunds where name = ? order by dt");
		FileReader tsfFR = new FileReader("tsfCorrelationTSFOverSignal_30days_MutualFunds.csv");
		BufferedReader tsfBR = new BufferedReader(tsfFR);

		String intsfLine = "";

		int daysOut = 30;
		int doubleBack = -1;
		tsfBR.readLine(); // skip header

		Averager avg = new Averager();
		String lastSym = "";
		// PriceBands priceBands = null;
		DeltaBands priceBands = null;
		TSFMakeARFFfromSQL makeSQL = new TSFMakeARFFfromSQL(false);
		while ((intsfLine = tsfBR.readLine()) != null) {
			String intsfData[] = intsfLine.split("_");

			String sym = intsfData[0];
			if (sym.compareTo(lastSym) != 0) {
				if (avg.getCount() > 0)
					System.out.println(avg.get());
				avg = new Averager();
				lastSym = sym;
				System.out.print(sym + ";");
			}

			psMFGet.setString(1, sym);
			daysOut = Integer.parseInt(intsfData[1]);

			ArrayList<Double> closes = new ArrayList<>();
			ArrayList<String> dates = new ArrayList<>();

			ResultSet rsMFGet = psMFGet.executeQuery();
			while (rsMFGet.next()) {
				closes.add((double) rsMFGet.getFloat(2));
				dates.add(rsMFGet.getString(1));

			}

			double dcloses[] = closes.stream().mapToDouble(dbl -> dbl).toArray();

			priceBands = new DeltaBands(dcloses, daysOut, 10);

			startDate = dates.get(50);

			TreeMap<String, double[]> tsfs = new TreeMap<>();
			TreeMap<String, Integer> tsfDaysDiff = new TreeMap<>();
			TreeMap<String, Integer> tsfDoubleBacks = new TreeMap<>();

			TreeMap<String, String[]> tsfDates = new TreeMap<>();
			StringWriter tsfSW = new StringWriter();
			PrintWriter tsfPW = new PrintWriter(tsfSW);
			tsfPW.println("% 1. Title: " + sym + "_tsf_correlation");
			tsfPW.println("@RELATION " + sym);

			for (int iread = 0; iread < 10; iread++) {
				intsfLine = tsfBR.readLine();
				// pos;abscorr;mfSym;priceDaysDiff;tsfSym;tsfPeriod;smDaysDiff;doubleBack;corr;tsfSym
				if (intsfLine.length() < 1)
					break;
				intsfData = intsfLine.split(";");
				if (intsfData.length < 2)
					break;
				String symbol = intsfData[1];
				if (intsfData[2].contains(sym) == false)
					continue;

				String symKey = intsfData[1] + "_" + intsfData[3];
				if (tsfs.containsKey(symKey))
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
				int tsfPeriod = Integer.parseInt(intsfData[3]);
				int daysDiff = Integer.parseInt(intsfData[4]);
				int tsfDiffDays = Integer.parseInt(intsfData[5]);
				doubleBack = Integer.parseInt(intsfData[6]);
				if (daysDiff != daysOut) {
					throw new Exception("days diff and out different");
				}
				Core core = new Core();
				MInteger outBegIdx = new MInteger();
				MInteger outNBElement = new MInteger();
				double[] tsf = new double[pgsd.inClose.length];
				core.tsf(0, pgsd.inClose.length - 1, pgsd.inClose, tsfPeriod, outBegIdx, outNBElement, tsf);
				// ArrayList<double[]> tsfands = new ArrayList<>();

				tsfs.put(symKey, tsf);
				tsfDaysDiff.put(symKey, tsfDiffDays);
				tsfDoubleBacks.put(symKey, doubleBack);
				tsfPW.println("@ATTRIBUTE " + symKey + "tsf NUMERIC");
				tsfPW.println("@ATTRIBUTE " + symKey + "tsfBack NUMERIC");
				tsfDates.put(symKey, pgsd.inDate);
			}
			tsfBR.readLine();
			{
				tsfPW.println(priceBands.getAttributeDefinition());
				tsfPW.println("@DATA");
				tsfPW.flush();
				StringWriter swtest = new StringWriter();
				PrintWriter pwtest = new PrintWriter(swtest);
				pwtest.print(tsfSW.toString());

				int arraypos[] = new int[tsfs.size()];
				int pos = 0;
				for (String key : tsfs.keySet()) {
					arraypos[pos] = 0;
				}
				pos = 50;
				while (dates.get(pos).compareTo(startDate) < 0)
					pos++;

				eemindexLoop: for (int iday = pos; iday < closes.size() - daysOut - 1;) {

					String edate = dates.get(iday);
					pos = 0;
					for (String key : tsfs.keySet()) {
						String sdate = tsfDates.get(key)[arraypos[pos]];
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

					makeSQL.printAttributeData(iday, daysOut, tsfPW, tsfs, tsfDaysDiff, tsfDoubleBacks, arraypos,
							dcloses, priceBands, false);

					pos = 0;
					for (pos = 0; pos < arraypos.length; pos++) {
						arraypos[pos]++;
					}

				}

				Instances instances = new Instances(new StringReader(tsfSW.toString()));
//				PrintWriter pw = new PrintWriter("c:/users/joe/correlationARFF/" + sym + "mftsf.arff");
//				pw.println(tsfSW.toString());
//				pw.flush();
//				pw.close();
				instances.setClassIndex(instances.numAttributes() - 1);
				// RandomForest classifier = new RandomForest();

				IBk classifier = new IBk();
				classifier.setKNN(2);

				classifier.buildClassifier(instances);

				pwtest.flush();

				for (int iday = 1; iday <= 5; iday++) {

					pos = 0;
					for (String key : tsfs.keySet()) {
						double[] tsf = tsfs.get(key);
						int tsfdaysdiff = tsfDaysDiff.get(key);
						int tsfstart = tsfDates.get(key).length - iday;
//						if (pos == 0)
//							System.out.println("\n" + daysOut + " using " + tsfDates.get(key)[tsfstart] + "; for ;"
//									+ (daysOut - (iday - 1)));
						;
						pwtest.print(makeSQL.getAttributeText(tsf, tsfdaysdiff, tsfstart, doubleBack) + ",");
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
