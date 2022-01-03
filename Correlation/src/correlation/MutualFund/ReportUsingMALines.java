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
import com.tictactec.ta.lib.MAType;

import bands.DeltaBands;
import correlation.MAAvgParms;
import correlation.MALinesMakeARFFfromSQLPhase2;
import correlation.MaLineParmToPass;
import movingAvgAndLines.MovingAvgAndLineIntercept;
import util.Averager;
import util.getDatabaseConnection;
import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class ReportUsingMALines {

	public static void main(String[] args) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		PreparedStatement psMFGet = conn
				.prepareStatement("select dt, close from fidelitymutualfunds where name = ? order by dt");
		FileReader maLinesFR = new FileReader("maLinesCorrelationOverSignal_30days_MutualFunds.csv");
		BufferedReader maLinesBR = new BufferedReader(maLinesFR);

		String inmaLinesLine = "";

		int daysOut = 30;

		maLinesBR.readLine(); // skip header

		Averager avg = new Averager();
		String lastSym = "";
		// PriceBands priceBands = null;
		DeltaBands priceBands = null;
		MALinesMakeARFFfromSQLPhase2 makeSQL = new MALinesMakeARFFfromSQLPhase2(false);
		while ((inmaLinesLine = maLinesBR.readLine()) != null) {

			String inmaLinesData[] = inmaLinesLine.split("_");

			String sym = inmaLinesData[0];
			if (sym.compareTo(lastSym) != 0) {
				if (avg.getCount() > 0)
					System.out.println(avg.get());
				avg = new Averager();
				lastSym = sym;
				System.out.print(sym + ";");
			}

			psMFGet.setString(1, sym);
			daysOut = Integer.parseInt(inmaLinesData[1]);

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

			StringWriter maLinesSW = new StringWriter();
			PrintWriter maLinesPW = new PrintWriter(maLinesSW);
			maLinesPW.println("% 1. Title: " + sym + "_maLines_correlation");
			maLinesPW.println("@RELATION " + sym);
			MAAvgParms maap = new MAAvgParms();
			for (int iread = 0; iread < 10; iread++) {
				inmaLinesLine = maLinesBR.readLine().trim();
				// pos;abscorr;mfSym;priceDaysDiff;maLinesSym;maLinesPeriod;smDaysDiff;doubleBack;corr;maLinesSym
				if (inmaLinesLine.length() < 1)
					break;
				if (inmaLinesLine.contains("not set"))
					continue;
				inmaLinesData = inmaLinesLine.split(";");
				if (inmaLinesData.length < 2)
					break;
				String symbol = inmaLinesData[1];
				if (inmaLinesData[2].contains(sym) == false)
					continue;

				GetETFDataUsingSQL pgsd;
				try {
					pgsd = GetETFDataUsingSQL.getInstance(symbol);
				} catch (Exception e) {
					// System.out.println(e.getMessage());
					continue;
				}

				// System.out.print(" " + symbol);
				int period = Integer.parseInt(inmaLinesData[3]);
				MAType maType = MAType.Sma;
				for (MAType type : MAType.values()) {
					if (type.name().compareTo(inmaLinesData[5]) == 0) {
						maType = type;
						break;
					}
				}
				String symKey = inmaLinesData[1] + "_" + inmaLinesData[3] + "_" + maType.name();
				MovingAvgAndLineIntercept mal = new MovingAvgAndLineIntercept(pgsd, period, maType, period, maType);
				MaLineParmToPass ptp = new MaLineParmToPass(mal, pgsd.inClose, null);
				maap.addSymbol(symKey);
				maap.setMALI(symKey, ptp);
				maap.setAttrDates(symKey, pgsd.inDate);
				maap.setLastDateStart(symKey, 200);
				maLinesPW.println("@ATTRIBUTE " + symKey + "maLines1 NUMERIC");
				maLinesPW.println("@ATTRIBUTE " + symKey + "maLines2 NUMERIC");
				maLinesPW.println("@ATTRIBUTE " + symKey + "maLines3 NUMERIC");
				// maLinesPW.println("@ATTRIBUTE " + symKey + "maLines4 NUMERIC");

			}
			maLinesBR.readLine();

			maLinesPW.println(priceBands.getAttributeDefinition());
			maLinesPW.println("@DATA");
			maLinesPW.flush();
			StringWriter swtest = new StringWriter();
			PrintWriter pwtest = new PrintWriter(swtest);
			pwtest.print(maLinesSW.toString().toString());

			for (int iday = 50; iday < closes.size() - daysOut - 1; iday++) {
				StringBuffer sb = makeSQL.printAttributeData(iday, ddates, daysOut, maap, dcloses, priceBands, false,
						false);
				if (sb != null)
					maLinesPW.print(sb.toString());

			}

			Instances instances = new Instances(new StringReader(maLinesSW.toString()));
			instances.setClassIndex(instances.numAttributes() - 1);

			Classifier classifier = new IBk();
			classifier.buildClassifier(instances);

			for (int iday = dcloses.length - 7; iday < dcloses.length; iday++) {
				StringBuffer sb = makeSQL.printAttributeData(iday, ddates, daysOut, maap, dcloses, priceBands, false,
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
