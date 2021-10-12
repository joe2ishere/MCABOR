package correlation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import util.Averager;
import util.getDatabaseConnection;
import utils.StandardDeviation;

public class PerformanceByGroup {

	public static void main(String[] args) throws Exception {

		Connection connRemote = getDatabaseConnection.makeMcVerryReportConnection();
		PreparedStatement psRemote = connRemote
				.prepareStatement("select currentReport, dateOfReport from forecastReport order by dateOfReport asc");
		Connection conn = getDatabaseConnection.makeConnection();

		PreparedStatement psGetGuesses = conn
				.prepareStatement("select  guesses from correlation30days where  symbol=? and mktDate = ? ");

		PreparedStatement psPriceClose = conn
				.prepareStatement("select close from prices where symbol = ?" + " and txn_date = ? ");

		PreparedStatement psPrices = conn.prepareStatement("select close from prices where symbol = ?"
				+ " and txn_date > ? ORDER BY txn_date ASC LIMIT 5 OFFSET ? ");

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		ResultSet rsRemote = psRemote.executeQuery();
		Pattern patt = Pattern.compile("(<tr><td>(\\w+)<\\/td>)((td\\d(B|S|-)){6})<\\/tr>");
		Pattern patt2 = Pattern.compile("td(\\d)(B|S|-)");

		ArrayList<TreeMap<Integer, Averager>> arrayResults = new ArrayList<TreeMap<Integer, Averager>>();
		ArrayList<TreeMap<Integer, Averager>> arrayResultsDev = new ArrayList<TreeMap<Integer, Averager>>();
		TreeMap<Integer, Averager> theResults;
		TreeMap<Integer, Averager> theResultsDev = null;
		for (int i = 0; i <= 5; i++) {
			theResults = new TreeMap<Integer, Averager>();
			theResultsDev = new TreeMap<Integer, Averager>();
			arrayResults.add(theResults);
			arrayResultsDev.add(theResultsDev);
			for (int ii = 0; ii <= 90; ii++) {
				theResults.put(ii, new Averager());
			}
			for (int ii = 0; ii <= 300; ii++) {
				theResultsDev.put(ii, new Averager());
			}
		}

		Integer weightG, weightGdev;
		while (rsRemote.next()) {
			String inGuess = rsRemote.getString(1);
			String mktDate = rsRemote.getString(2);
			// <tr><td>amlp</td>td1Btd2-td3-td4-td5-td6-</tr>
			Matcher match = patt.matcher(inGuess);

			while (match.find()) {
				String sym = match.group(2);

				psGetGuesses.setString(1, sym);
				psGetGuesses.setString(2, mktDate);
				ResultSet rsGuess = psGetGuesses.executeQuery();
				if (rsGuess.next() == false) {
					// throw new Exception("logic error, date not found in guess database " +
					// mktDate);
					continue;
				}
				String guesses = rsGuess.getString(1);
				String guessArray[] = guesses.split(";");

				psPriceClose.setString(1, sym);
				psPriceClose.setString(2, mktDate);
				ResultSet rsPriceClose = psPriceClose.executeQuery();
				if (rsPriceClose.next() == false)
					continue;
				double pc = rsPriceClose.getDouble(1);

				Matcher match2 = patt2.matcher(match.group(3));
				while (match2.find()) {
					if (match2.group(2).startsWith("-") == true)
						continue;
					Averager pavg = new Averager();
					Averager avgGuess = new Averager();
					StandardDeviation stdDev = new StandardDeviation();
					{
						int weeksOut = Integer.parseInt(match2.group(1));

						weeksOut--;
						for (int ig = weeksOut * 5; ig < weeksOut * 5 + 5; ig++) {
							avgGuess.add(Double.parseDouble(guessArray[ig]));
							stdDev.enter(Double.parseDouble(guessArray[ig]));
						}
						theResults = arrayResults.get(weeksOut);
						theResultsDev = arrayResultsDev.get(weeksOut);
						weeksOut *= 5;

						Double d = avgGuess.get() * 10;
						weightG = d.intValue();
						Double ddev = stdDev.getStandardDeviation() * 100;
						weightGdev = ddev.intValue();

						psPrices.setString(1, sym);
						psPrices.setString(2, mktDate);
						psPrices.setInt(3, weeksOut);

						ResultSet rsPrices = psPrices.executeQuery();
						while (rsPrices.next()) {
							pavg.add(rsPrices.getDouble(1));
						}
					}

					if (pavg.getCount() <= 4)
						continue;

					Averager avg = theResults.get(weightG);
					Averager avgdev = theResultsDev.get(weightGdev);
					if (match2.group(2).startsWith("B"))
						if (pavg.get() >= pc) {
							avg.add(1.0);
							avgdev.add(1.0);
						} else {
							avg.add(.0);
							avgdev.add(.0);

						}

					if (match2.group(2).startsWith("S"))
						if (pavg.get() <= pc) {
							avg.add(1.0);
							avgdev.add(1.0);
						} else {
							avg.add(.0);
							avgdev.add(.0);

						}

				}

			}

		}
		for (int ii = 0; ii <= 90; ii++) {
			System.out.print(ii + ";");
			for (int i = 0; i <= 5; i++) {
				theResults = arrayResults.get(i);

				if (theResults.get(ii).getCount() > 0)
					System.out.print(theResults.get(ii).get() + ";");// + theResults.get(key).getCount());
				else
					System.out.print(";");
			}
			System.out.println();
		}
//		for (int ii = 0; ii <= 300; ii++) {
//			System.out.print(ii + ";");
//			for (int i = 0; i <= 5; i++) {
//				theResultsDev = arrayResultsDev.get(i);
//				if (theResultsDev.get(ii).getCount() > 2)
//					System.out.print(theResultsDev.get(ii).get() + ";");// + theResults.get(key).getCount());
//				else
//					System.out.print(";");
//			}
//			System.out.println();
//		}
	}

}
