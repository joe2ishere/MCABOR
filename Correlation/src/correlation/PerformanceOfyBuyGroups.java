package correlation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import util.Averager;
import util.getDatabaseConnection;
import utils.StandardDeviation;
import utils.TopAndBottomList;

public class PerformanceOfyBuyGroups {

	public static void main(String[] args) throws Exception {

		DecimalFormat df0 = new DecimalFormat("# ");
		DecimalFormat df = new DecimalFormat("#.0");
		DecimalFormat df3 = new DecimalFormat("#.000");
		Connection connRemote = getDatabaseConnection.makeMcVerryReportConnection();
		PreparedStatement psRemote = connRemote.prepareStatement(
				"select currentReport, dateOfReport from forecastReport where currentReport like ? ORDER BY dateOfReport asc");
		Connection conn = getDatabaseConnection.makeConnection();

		PreparedStatement psGetGuesses = conn
				.prepareStatement("select  guesses from correlation30days where  symbol=? and mktDate = ? ");

		PreparedStatement psPriceClose = conn.prepareStatement("select high, low, close from prices where symbol = ?"
				+ " and txn_date >= ? order by txn_date limit 2");

		PreparedStatement psPrices = conn.prepareStatement("select close from prices where symbol = ?"
				+ " and txn_date > ? ORDER BY txn_date ASC LIMIT ? OFFSET ? ");

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		PrintWriter symavgpw = new PrintWriter("symavgpw.csv");
		TreeMap<String, Averager> symAverager;
		// '%td1Btd2Btd3BTD4BTD5B%'
		for (int groupCnt = 1; groupCnt <= 6; groupCnt++) {
			Averager groupAverage = new Averager();
			Averager groupSuccess = new Averager();
			for (int weeksOut = 1; weeksOut <= 6; weeksOut++) {
				Averager weekAverage = new Averager();
				Averager weekSuccess = new Averager();
				TopAndBottomList best = new TopAndBottomList();
				TopAndBottomList worst = new TopAndBottomList();
				symAverager = new TreeMap<String, Averager>();
				int dbWeeksOffset = weeksOut * 5 + (groupCnt - 1) * 5;
				int dbWeeksLimit = dbWeeksOffset + 5;
				String ir$ = "td" + weeksOut + "Btd";
				for (int igc = 2; igc < groupCnt; igc++)
					ir$ += (weeksOut + igc - 1) + "Btd";
				ir$ += (weeksOut + groupCnt - 1) + "B";
				if (groupCnt == 1)
					ir$ = "td" + weeksOut + "B";

				psRemote.setString(1, "%" + ir$ + "%");
				if (ir$.contains("7"))
					continue;

				ir$ = "";
				for (int wkc = 1; wkc < weeksOut; wkc++)
					ir$ += "td" + wkc + "(S|-)";

				for (int gc = 0; gc < groupCnt; gc++)
					ir$ += "td" + (weeksOut + gc) + "B";
				for (int wkc = groupCnt + weeksOut; wkc < 7; wkc++)
					ir$ += "td" + (wkc) + "(S|-)";

				ResultSet rsRemote = psRemote.executeQuery();
				Pattern patt = Pattern.compile("(<tr><td>(\\w+)<\\/td>)(" + ir$ + ")<\\/tr>");
				Pattern patt2 = Pattern.compile(ir$);

				Integer weightG;
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
						if (rsPriceClose.next() == false)
							continue;
						double pc = (rsPriceClose.getDouble(1) + rsPriceClose.getDouble(1) + rsPriceClose.getDouble(1))
								/ 3;

						Matcher match2 = patt2.matcher(match.group(3));
						while (match2.find()) {
//						 
							Averager pavg = new Averager();
							Averager avgGuess = new Averager();
							StandardDeviation stdDev = new StandardDeviation();
							{

								int iweeksOut = weeksOut;
								for (int ig = iweeksOut * 5; ig < iweeksOut * 5 + 5; ig++) {
									if (ig >= guessArray.length)
										break;
									avgGuess.add(Double.parseDouble(guessArray[ig]));
									stdDev.enter(Double.parseDouble(guessArray[ig]));
								}

								iweeksOut *= 5;

								Double d = avgGuess.get() * 10;
								weightG = d.intValue();

								psPrices.setString(1, sym);
								psPrices.setString(2, mktDate);
								psPrices.setInt(3, dbWeeksLimit);
								psPrices.setInt(4, dbWeeksOffset);

								ResultSet rsPrices = psPrices.executeQuery();
								while (rsPrices.next()) {
									pavg.add(rsPrices.getDouble(1));
								}
							}

							if (pavg.getCount() < dbWeeksLimit)
								continue;

							// if (match2.group(2).startsWith("B"))
							if (pavg.get() >= pc) {
								weekSuccess.add(1.0);
								groupSuccess.add(1.0);

							} else {
								weekSuccess.add(.0);
								groupSuccess.add(.0);

							}
							weekAverage.add(pavg.get() / pc);
							groupAverage.add(pavg.get() / pc);
							best.setTop(pavg.get() / pc, "<td>" + sym + "<td>" + mktDate);
							worst.setBottom(pavg.get() / pc, "<td>" + sym + "<td>" + mktDate);
							Averager symavg = symAverager.get(sym);
							if (symavg == null) {
								symavg = new Averager();
								symAverager.put(sym, symavg);
							}
							symavg.add(pavg.get() / pc);
						}

					}

				}
				System.out.println("<tr><td>" + groupCnt + "<td>" + weeksOut + "<td style='color:"
						+ (weekAverage.get() < 1 ? "red" : "green") + "'>" + df.format(100 * (weekAverage.get() - 1))
						+ "<td>" + df.format(100 * weekSuccess.get()) + "<td>" + df0.format(weekAverage.getCount())
						+ best.getTopDescription()[0] + "<td>" + df.format(100 * (best.getTopValue(0) - 1))
						+ worst.getBottomDescription()[0] + "<td style='color:"
						+ (worst.getBottomValue(0) < 1 ? "red" : "green") + "'>"
						+ df.format(100 * (worst.getBottomValue(0) - 1)));
				for (String key : symAverager.keySet()) {
					symavgpw.println(
							groupCnt + ";" + weeksOut + ";" + key + ";" + df3.format(symAverager.get(key).get() - 1)
									+ ";" + df0.format(symAverager.get(key).getCount()));
				}
				symavgpw.flush();
			}

			System.out.println("<tr><td>" + groupCnt + "<td>all<td style='color:green'>"
					+ df.format(100 * (groupAverage.get() - 1)) + "<td>" + df.format(100 * groupSuccess.get()) + "<td>"
					+ df0.format(groupAverage.getCount()));
		}
//		for (int ii = 0; ii <= 90; ii++) {
//			System.out.print(ii + ";");
//			for (int i = 0; i <= 5; i++) {
//				theResults = arrayResults.get(i);
//				theResultsGain = arrayResultsGain.get(i);
//				if (theResults.get(ii).getCount() > 0)
//					System.out.print(theResults.get(ii).get() + ";" + theResultsGain.get(ii).get() + ";");
//				else
//					System.out.print(";;");
//			}
//			System.out.println();
//		}
	}

}
