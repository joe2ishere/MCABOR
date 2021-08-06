package correlation.MutualFund;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;

import correlation.Correlation;
import util.getDatabaseConnection;
import utils.TopAndBottomList;

public class MACDCorrelationMutualFunds implements Runnable {

	static TreeMap<String, GetETFDataUsingSQL> gsds = new TreeMap<>();
	static TreeMap<String, TopAndBottomList> symbolsBest = new TreeMap<>();
	static TreeMap<String, ArrayList<Double>> mfcloses = new TreeMap<>();
	static TreeMap<String, ArrayList<String>> mfdates = new TreeMap<>();

	public static void main(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();
		PreparedStatement ps = conn.prepareStatement(
				"select symbol, AVG(volume) AS vavg, count(*) as txncnt  from etfprices  GROUP BY symbol ORDER BY symbol");

		ResultSet rs = ps.executeQuery();

		while (rs.next()) {

			String sym = rs.getString(1).toLowerCase();
			double vol = rs.getDouble(2);
			int cnt = rs.getInt(3);

			if (vol < 25000) {
				System.out.println("skipping " + sym + " with vol of " + vol);
				continue;
			}

			if (cnt < 2000) {
				System.out.println("skipping " + sym + " with cnt of " + cnt);
				continue;

			}

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(sym);

			gsds.put(sym, pgsd);

		}

		PreparedStatement pmacdF = conn.prepareStatement("select distinct name from fidelitymutualfunds");
		PreparedStatement pmacdFGet = conn
				.prepareStatement("select dt, close from fidelitymutualfunds where name = ? order by dt");
		ResultSet rmacdF = pmacdF.executeQuery();

		while (rmacdF.next()) {
			String mfName = rmacdF.getString(1);
			pmacdFGet.setString(1, mfName);
			ArrayList<Double> closes = new ArrayList<>();
			ArrayList<String> dates = new ArrayList<>();
			mfcloses.put(mfName, closes);
			mfdates.put(mfName, dates);
			ResultSet rmacdFGet = pmacdFGet.executeQuery();
			while (rmacdFGet.next()) {
				closes.add((double) rmacdFGet.getFloat(2));
				dates.add(rmacdFGet.getString(1));
			}

		}

		BlockingQueue<String> macdiQue = new ArrayBlockingQueue<String>(4);
		MACDCorrelationMutualFunds corrs = new MACDCorrelationMutualFunds(macdiQue);

		for (int i = 0; i < 4; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}

		for (String macdiSym : gsds.keySet()) {
//			if (macdiSym.compareTo("c") > 0)
//				break;

			macdiQue.put(macdiSym);

		}

		for (int i = 0; i < 4; i++) {
			macdiQue.put("<<<STOP>>>");
		}
		while (macdiQue.isEmpty() == false)
			Thread.sleep(1000);

		writeFile();

	}

	BlockingQueue<String> macdiQue;

	public MACDCorrelationMutualFunds(BlockingQueue<String> macdiQue) {
		this.macdiQue = macdiQue;

	}

	public static void writeFile() throws FileNotFoundException {
		PrintWriter pw = new PrintWriter("macdCorrelationMACDOverSignal" + "_30days_" + "MutualFunds.csv");
		pw.println("abs Corr;macd Sym;close Sym;macd period;to Close Days Diff;macd Days Diff;double Back; true Corr");

		for (String sym : symbolsBest.keySet()) {
			pw.println(sym);
			TopAndBottomList tab = symbolsBest.get(sym);
			pw.println(tab.getTop());
		}
		pw.flush();
		pw.close();
	}

	@Override
	public void run() {

		try {
			while (true) {
				String macdiSym = macdiQue.take();
				if (macdiSym.contains("<<<STOP>>>"))
					return;

				GetETFDataUsingSQL macdGSD = gsds.get(macdiSym);

				System.out.println("running with " + macdGSD.getSymbol());
				Core core = new Core();
				for (int optInFastPeriod = 10; optInFastPeriod <= 16; optInFastPeriod += 1) {
					for (int optInSlowPeriod = 9; optInSlowPeriod <= 15; optInSlowPeriod += 1) {
						if (optInSlowPeriod >= optInFastPeriod)
							continue;
						for (int optInSignalPeriod = 2; optInSignalPeriod <= 7; optInSignalPeriod += 1) {

							MInteger outBegIdx = new MInteger();
							MInteger outNBElement = new MInteger();
							double outMACD[] = new double[macdGSD.inClose.length];
							double[] outMACDSignal = new double[macdGSD.inClose.length];
							double[] outMACDHist = new double[macdGSD.inClose.length];
							core.macd(0, macdGSD.inClose.length - 1, macdGSD.inClose, optInFastPeriod, optInSlowPeriod,
									optInSignalPeriod, outBegIdx, outNBElement, outMACD, outMACDSignal, outMACDHist);

							for (int priceDaysDiff = 30; priceDaysDiff <= 45; priceDaysDiff += 5) {
								for (int macdDaysDiff = 1; macdDaysDiff <= 3; macdDaysDiff += 1) {
									for (String closeSym : mfcloses.keySet()) {

										ArrayList<Double> closes = mfcloses.get(closeSym);
										ArrayList<String> dates = mfdates.get(closeSym);

										{
											for (int doubleBack = 0; doubleBack < 5; doubleBack++) {
												ArrayList<Double> ccArray1 = new ArrayList<Double>(),
														ccArray2 = new ArrayList<Double>();

												for (int pgsdDateIndex = 50, mfDateIndex = 50; mfDateIndex < macdGSD.inDate.length
														- priceDaysDiff
														& pgsdDateIndex < dates.size() - priceDaysDiff;) {
													int dateCompare = macdGSD.inDate[mfDateIndex]
															.compareTo(dates.get(pgsdDateIndex));
													if (dateCompare < 0) {
														mfDateIndex++;
														continue;
													}
													if (dateCompare > 0) {
														pgsdDateIndex++;
														continue;
													}

													ccArray1.add(closes.get(mfDateIndex + priceDaysDiff)
															/ closes.get(mfDateIndex));
													ccArray2.add((outMACD[mfDateIndex - (doubleBack)]
															+ outMACD[mfDateIndex - (macdDaysDiff + doubleBack)]) / 2);

													pgsdDateIndex++;
													mfDateIndex++;
												}

												double corr = Correlation.pearsonsCorrelation(ccArray1, ccArray2);
												if (Double.isInfinite(corr))
													continue;

												if (Double.isNaN(corr))
													continue;
												synchronized (symbolsBest) {
													TopAndBottomList tab = symbolsBest
															.get(closeSym + "_" + priceDaysDiff);
													if (tab == null)
														tab = new TopAndBottomList();
													tab.setTop(Math.abs(corr),
															macdGSD.getSymbol() + ";" + closeSym + ";" + optInFastPeriod
																	+ ";" + optInSlowPeriod + ";" + optInSignalPeriod
																	+ ";" + +priceDaysDiff + ";" + macdDaysDiff + ";"
																	+ doubleBack + ";" + df.format(corr));

													symbolsBest.put(closeSym + "_" + priceDaysDiff, tab);
												}
											}
										}
									}

								}
							}
						}
					}
				}
				synchronized (macdGSD) {
					writeFile();
					System.out.println("done with " + macdGSD.getSymbol());
				}

			}
		} catch (

				InterruptedException | FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
	}

	static DecimalFormat df = new DecimalFormat("0.0000");
}