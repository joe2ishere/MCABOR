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
import com.tictactec.ta.lib.MAType;

import StochasticMomentum.StochasticMomentum;
import bands.DeltaBands;
import correlation.SMMakeARFFfromCorrelationFile;
import util.Averager;
import util.getDatabaseConnection;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class MakeMFBuySellReportUsingSMI {

	public static void main(String[] args) throws Exception {

		String startDate;
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		PreparedStatement psMFGet = conn
				.prepareStatement("select dt, close from fidelitymutualfunds where name = ? order by dt");
		FileReader smiFR = new FileReader("smiCorrelationSMIoverSignal_30days_MutualFunds.csv");
		BufferedReader smiBR = new BufferedReader(smiFR);

		String inSMILine = "";

		int daysOut = 30;
		int doubleBack = -1;
		smiBR.readLine(); // skip header

		Averager avg = new Averager();
		String lastSym = "";
		// PriceBands priceBands = null;
		DeltaBands priceBands = null;
		while ((inSMILine = smiBR.readLine()) != null) {
			String inSMIData[] = inSMILine.split("_");
			String sym = inSMIData[0];
			if (sym.compareTo(lastSym) != 0) {
				if (avg.getCount() > 0)
					System.out.println(avg.get());
				avg = new Averager();
				lastSym = sym;
				System.out.print(sym + ";");
			}
			psMFGet.setString(1, sym);
			daysOut = Integer.parseInt(inSMIData[1]);

			ArrayList<Double> closes = new ArrayList<>();
			ArrayList<String> dates = new ArrayList<>();

			ResultSet rsMFGet = psMFGet.executeQuery();
			while (rsMFGet.next()) {
				closes.add((double) rsMFGet.getFloat(2));
				dates.add(rsMFGet.getString(1));

			}

			double dcloses[] = closes.stream().mapToDouble(dbl -> dbl).toArray();
			// priceBands = new PriceBands(dcloses, daysOut);
			priceBands = new DeltaBands(dcloses, daysOut, 10);

			startDate = dates.get(50);

			TreeMap<String, StochasticMomentum> smis = new TreeMap<>();
			TreeMap<String, Integer> smDaysDiff = new TreeMap<>();

			TreeMap<String, String[]> smDates = new TreeMap<>();
			StringWriter smiSW = new StringWriter();
			PrintWriter smiPW = new PrintWriter(smiSW);
			smiPW.println("% 1. Title: " + sym + "_smi_correlation");
			smiPW.println("@RELATION " + sym);

			for (int iread = 0; iread < 10; iread++) {
				inSMILine = smiBR.readLine();
				if (inSMILine.length() < 1)
					break;
				inSMIData = inSMILine.split(";");
				if (inSMIData.length < 2)
					break;
				String symbol = inSMIData[1];
				if (inSMIData[2].contains(sym) == false)
					continue;

				String symKey = inSMIData[1] + "_" + inSMIData[0];
				if (smis.containsKey(symKey))
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
				int hiLowPeriod = Integer.parseInt(inSMIData[3]);
				int maPeriod = Integer.parseInt(inSMIData[4]);
				int smSmoothPeriod = Integer.parseInt(inSMIData[5]);
				int smSignalPeriod = Integer.parseInt(inSMIData[6]);
				int daysDiff = Integer.parseInt(inSMIData[7]);
				doubleBack = Integer.parseInt(inSMIData[8]);
				if (daysDiff != daysOut) {
					throw new Exception("days diff and out different");
				}
				StochasticMomentum sm = new StochasticMomentum(pgsd.inHigh, pgsd.inLow, pgsd.inClose, hiLowPeriod,
						MAType.Ema, maPeriod, smSmoothPeriod, smSignalPeriod);
				smis.put(symKey, sm);
				smDaysDiff.put(symKey, Integer.parseInt(inSMIData[6]));
				smiPW.println("@ATTRIBUTE " + symKey + "smi NUMERIC");
				smiPW.println("@ATTRIBUTE " + symKey + "smiRF {R,F}");
				smiPW.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
				smiPW.println("@ATTRIBUTE " + symKey + "signalRF {R,F}");
				smDates.put(symKey, pgsd.inDate);
			}
			smiBR.readLine();
			{
				smiPW.println(priceBands.getAttributeDefinition());
				smiPW.println("@DATA");
				smiPW.flush();
				StringWriter swtest = new StringWriter();
				PrintWriter pwtest = new PrintWriter(swtest);
				pwtest.print(smiSW.toString());

				int arraypos[] = new int[smis.size()];
				int pos = 50;
				while (dates.get(pos).compareTo(startDate) < 0)
					pos++;

				eemindexLoop: for (int iday = pos; iday < closes.size() - daysOut - 1;) {

					String edate = dates.get(iday);
					pos = 0;
					for (String key : smis.keySet()) {
						String sdate = smDates.get(key)[arraypos[pos]];
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

					SMMakeARFFfromCorrelationFile.printAttributeData(iday, daysOut, smiPW, smis, smDaysDiff, arraypos,
							dcloses, doubleBack, priceBands);

					pos = 0;
					for (pos = 0; pos < arraypos.length; pos++) {
						arraypos[pos]++;
					}

				}

				Instances instances = new Instances(new StringReader(smiSW.toString()));
//				PrintWriter pw = new PrintWriter("c:/users/joe/correlationARFF/" + sym + "mfsmi.arff");
//				pw.println(smiSW.toString());
//				pw.flush();
//				pw.close();
				instances.setClassIndex(instances.numAttributes() - 1);
				// RandomForest classifier = new RandomForest();
				IBk classifier = new IBk();
				classifier.buildClassifier(instances);

				pwtest.flush();

				for (int iday = 1; iday <= 5; iday++) {

					pos = 0;
					for (String key : smis.keySet()) {
						StochasticMomentum sm = smis.get(key);
						int smdaysdiff = smDaysDiff.get(key);
						int smstart = smDates.get(key).length - iday;
//						if (pos == 0)
//							System.out.println("\n" + daysOut + " using " + smDates.get(key)[smstart] + "; for ;"
//									+ (daysOut - (iday - 1)));
						;
						pwtest.print(SMMakeARFFfromCorrelationFile.getAttributeText(sm, smdaysdiff, smstart, doubleBack)
								+ ",");
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
