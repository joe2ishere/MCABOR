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

import bands.DeltaBands;
import correlation.MALinesMakeARFFfromSQL;
import correlation.MaLineParmToPass;
import movingAvgAndLines.MovingAvgAndLineIntercept;
import util.Averager;
import util.getDatabaseConnection;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;

public class MakeMFBuySellReportUsingMALines {

	public static void main(String[] args) throws Exception {

		String startDate;
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		PreparedStatement psMFGet = conn
				.prepareStatement("select dt, close from fidelitymutualfunds where name = ? order by dt");
		FileReader maLinesFR = new FileReader("maLinesCorrelationOverSignal_30days_MutualFunds.csv");
		BufferedReader maLinesBR = new BufferedReader(maLinesFR);

		String inmaLinesLine = "";

		int daysOut = 30;
		int doubleBack = -1;
		maLinesBR.readLine(); // skip header

		Averager avg = new Averager();
		String lastSym = "";
		// PriceBands priceBands = null;
		DeltaBands priceBands = null;
		MALinesMakeARFFfromSQL makeSQL = new MALinesMakeARFFfromSQL(false);
		while ((inmaLinesLine = maLinesBR.readLine()) != null) {

			TreeMap<String, Object> malis = new TreeMap<>();
			TreeMap<String, String[]> malisDates = new TreeMap<String, String[]>();

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

			priceBands = new DeltaBands(dcloses, daysOut, 10);

			startDate = dates.get(50);

			StringWriter maLinesSW = new StringWriter();
			PrintWriter maLinesPW = new PrintWriter(maLinesSW);
			maLinesPW.println("% 1. Title: " + sym + "_maLines_correlation");
			maLinesPW.println("@RELATION " + sym);

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

				if (startDate.compareTo(pgsd.inDate[200]) < 0)
					startDate = pgsd.inDate[200];

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
				malis.put(symKey, ptp);
				malisDates.put(symKey, pgsd.inDate);
				maLinesPW.println("@ATTRIBUTE " + symKey + "maLines1 NUMERIC");
				maLinesPW.println("@ATTRIBUTE " + symKey + "maLines2 NUMERIC");
				maLinesPW.println("@ATTRIBUTE " + symKey + "maLines3 NUMERIC");
				// maLinesPW.println("@ATTRIBUTE " + symKey + "maLines4 NUMERIC");

			}
			maLinesBR.readLine();
			{
				maLinesPW.println(priceBands.getAttributeDefinition());
				maLinesPW.println("@DATA");
				maLinesPW.flush();

				int arraypos[] = new int[malis.size()];
				int pos = 200;
				while (dates.get(pos).compareTo(startDate) < 0)
					pos++;

				eemindexLoop: for (int iday = pos; iday < closes.size() - daysOut - 1;) {
					String sdate = "";
					String edate = dates.get(iday);
					pos = 0;
					for (String key : malis.keySet()) {
						sdate = malisDates.get(key)[arraypos[pos]];
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

					makeSQL.printAttributeData(iday, daysOut, maLinesPW, malis, arraypos, dcloses, edate, priceBands,
							false);

					pos = 0;
					for (pos = 0; pos < arraypos.length; pos++) {
						arraypos[pos]++;
					}

				}

				Instances instances = new Instances(new StringReader(maLinesSW.toString()));

				instances.setClassIndex(instances.numAttributes() - 1);

				RandomForest classifier = new RandomForest();

				classifier.buildClassifier(instances);

				for (int iday = 1; iday <= 5; iday++) {

					pos = 0;
					for (String key : malis.keySet()) {

						MaLineParmToPass ptp = (MaLineParmToPass) malis.get(key);
						ptp.processDate = dates.get(dates.size() - iday);
						makeSQL.getAttributeText(maLinesPW, ptp, daysOut);

						arraypos[pos]++;
						pos++;

					}

					maLinesPW.println("?");
					maLinesPW.flush();
				}

				PrintWriter pwo = new PrintWriter("/users/joe/correlationarff/" + sym + "mf.arff");
				pwo.print(maLinesSW.toString());
				pwo.flush();
				pwo.close();

				Instances instancesTest = new Instances(new StringReader(maLinesSW.toString()));

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
