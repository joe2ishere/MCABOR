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
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import correlation.TSFMakeARFFfromSQLPhase2;
import correlation.TSFParms;
import util.Averager;
import util.getDatabaseConnection;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class ReportUsingTSF {

	public static void main(String[] args) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		PreparedStatement psMFGet = conn
				.prepareStatement("select dt, close from fidelitymutualfunds where name = ? order by dt");
		FileReader tsfFR = new FileReader("tsfCorrelationTSFOverSignal_30days_MutualFunds.csv");
		BufferedReader tsfBR = new BufferedReader(tsfFR);

		String intsfLine = "";

		int daysOut = 30;

		tsfBR.readLine(); // skip header

		Averager avg = new Averager();
		String lastSym = "";
		// PriceBands priceBands = null;
		DeltaBands priceBands = null;
		TSFMakeARFFfromSQLPhase2 makeSQL = new TSFMakeARFFfromSQLPhase2(false, true);
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
			String ddates[] = dates.toArray(new String[0]);

			priceBands = new DeltaBands(dcloses, daysOut, 10);

			StringWriter tsfSW = new StringWriter();
			PrintWriter tsfPW = new PrintWriter(tsfSW);
			tsfPW.println("% 1. Title: " + sym + "_tsf_correlation");
			tsfPW.println("@RELATION " + sym + daysOut);
			TSFParms tsfp = new TSFParms();
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

				String symKey = intsfData[1] + "_" + intsfData[3] + "_" + intsfData[5];
				if (tsfp.keySet().contains(symKey))
					continue;
				GetETFDataUsingSQL pgsd;
				try {
					pgsd = GetETFDataUsingSQL.getInstance(symbol);
				} catch (Exception e) {
					// System.out.println(e.getMessage());
					continue;
				}

				// System.out.print(" " + symbol);
				int tsfPeriod = Integer.parseInt(intsfData[3]);
				int daysDiff = Integer.parseInt(intsfData[4]);
				int tsfDiffDays = Integer.parseInt(intsfData[5]);

				if (daysDiff != daysOut) {
					throw new Exception("days diff and out different");
				}
				Core core = new Core();
				MInteger outBegIdx = new MInteger();
				MInteger outNBElement = new MInteger();
				double[] tsf = new double[pgsd.inClose.length];
				core.tsf(0, pgsd.inClose.length - 1, pgsd.inClose, tsfPeriod, outBegIdx, outNBElement, tsf);
				// ArrayList<double[]> tsfands = new ArrayList<>();
				tsfp.addSymbol(symKey);
				tsfp.settsfs(symKey, tsf);
				tsfp.setDoubleBacks(symKey, daysDiff);
				tsfp.setDaysDiff(symKey, tsfDiffDays);
				tsfp.setAttrDates(symKey, pgsd.inDate);
				tsfp.setLastDateStart(symKey, 75);

				tsfPW.println("@ATTRIBUTE " + symKey + "tsf NUMERIC");
				tsfPW.println("@ATTRIBUTE " + symKey + "tsfBack NUMERIC");

			}
			tsfBR.readLine();

			tsfPW.println(priceBands.getAttributeDefinition());
			tsfPW.println("@DATA");
			tsfPW.flush();
			StringWriter swtest = new StringWriter();
			PrintWriter pwtest = new PrintWriter(swtest);
			pwtest.print(tsfSW.toString().toString());

			for (int iday = 50; iday < dcloses.length - daysOut - 1; iday++) {
				StringBuffer sb = makeSQL.printAttributeData(iday, ddates, daysOut, tsfp, dcloses, priceBands, false,
						false);
				if (sb != null)
					tsfPW.print(sb.toString());
			}

			Instances instances = new Instances(new StringReader(tsfSW.toString()));
			PrintWriter pw = new PrintWriter("c:/users/joe/correlationARFF/" + sym + "mftsf.arff");
			pw.println(tsfSW.toString());
			pw.flush();
			pw.close();
			instances.setClassIndex(instances.numAttributes() - 1);
			// RandomForest classifier = new RandomForest();

			IBk classifier = new IBk();
			classifier.setKNN(2);
			classifier.buildClassifier(instances);

			for (int iday = dcloses.length - 7; iday < dcloses.length; iday++) {
				StringBuffer sb = makeSQL.printAttributeData(iday, ddates, daysOut, tsfp, dcloses, priceBands, false,
						true);
				if (sb != null)
					pwtest.println(sb.toString());

			}
			Instances instancesTest = new Instances(new StringReader(swtest.toString()));

			instancesTest.setClassIndex(instancesTest.numAttributes() - 1);
			for (int i = 0; i < instancesTest.size(); i++) {
				double got = classifier.classifyInstance(instancesTest.get(i));

				avg.add(priceBands.relativeTo9(got));

			}

		}
		System.out.println(avg.get());
	}

}
