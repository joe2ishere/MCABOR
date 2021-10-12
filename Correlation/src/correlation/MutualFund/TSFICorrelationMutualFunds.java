package correlation.MutualFund;

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

public class TSFICorrelationMutualFunds implements Runnable {

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

		PreparedStatement ptsfF = conn.prepareStatement("select distinct name from fidelitymutualfunds");
		PreparedStatement ptsfFGet = conn
				.prepareStatement("select dt, close from fidelitymutualfunds where name = ? order by dt");
		ResultSet rtsfF = ptsfF.executeQuery();

		while (rtsfF.next()) {
			String mfName = rtsfF.getString(1);
			ptsfFGet.setString(1, mfName);
			ArrayList<Double> closes = new ArrayList<>();
			ArrayList<String> dates = new ArrayList<>();
			mfcloses.put(mfName, closes);
			mfdates.put(mfName, dates);
			ResultSet rtsfFGet = ptsfFGet.executeQuery();
			while (rtsfFGet.next()) {
				closes.add((double) rtsfFGet.getFloat(2));
				dates.add(rtsfFGet.getString(1));
			}

		}

		BlockingQueue<String> tsfiQue = new ArrayBlockingQueue<String>(4);
		TSFICorrelationMutualFunds corrs = new TSFICorrelationMutualFunds(tsfiQue);

		for (int i = 0; i < 4; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}

		for (String tsfiSym : gsds.keySet()) {
//			if (tsfiSym.compareTo("c") > 0)
//				break;

			tsfiQue.put(tsfiSym);

		}

		for (int i = 0; i < 4; i++) {
			tsfiQue.put("<<<STOP>>>");
		}
		while (tsfiQue.isEmpty() == false)
			Thread.sleep(1000);

		PrintWriter pw = new PrintWriter("tsfCorrelationOverSignal" + "_30days_" + "MutualFunds.csv");
		pw.println("abs Corr;tsf Sym;close Sym;tsf period;to Close Days Diff;tsf Days Diff;double Back; true Corr");

		for (String sym : symbolsBest.keySet()) {
			pw.println(sym);
			TopAndBottomList tab = symbolsBest.get(sym);
			pw.println(tab.getTop());
		}
		pw.flush();
		pw.close();

	}

	BlockingQueue<String> tsfiQue;

	public TSFICorrelationMutualFunds(BlockingQueue<String> tsfiQue) {
		this.tsfiQue = tsfiQue;

	}

	@Override
	public void run() {

		try {
			while (true) {
				String tsfiSym = tsfiQue.take();
				if (tsfiSym.contains("<<<STOP>>>"))
					return;

				GetETFDataUsingSQL pgsd = gsds.get(tsfiSym);

				System.out.println("running with " + pgsd.getSymbol());
				Core core = new Core();
				for (int tsfPeriod = 2; tsfPeriod <= 24; tsfPeriod += 1) {
					double tsf[] = new double[pgsd.inClose.length];
					MInteger outBegIdx = new MInteger();
					MInteger outNBElement = new MInteger();
					core.tsf(0, pgsd.inClose.length - 1, pgsd.inClose, tsfPeriod, outBegIdx, outNBElement, tsf);

					for (int priceDaysDiff = 30; priceDaysDiff <= 45; priceDaysDiff += 5) {
						for (int tsfDaysDiff = 1; tsfDaysDiff <= 20; tsfDaysDiff += 1) {
							for (String closeSym : mfcloses.keySet()) {

								ArrayList<Double> closes = mfcloses.get(closeSym);
								ArrayList<String> dates = mfdates.get(closeSym);

								{
									for (int doubleBack = 0; doubleBack < 5; doubleBack++) {
										ArrayList<Double> ccArray1 = new ArrayList<Double>(),
												ccArray2 = new ArrayList<Double>();

										for (int pgsdDateIndex = 50, mfDateIndex = 50; pgsdDateIndex < pgsd.inDate.length
												- priceDaysDiff & mfDateIndex < dates.size() - priceDaysDiff;) {
											int dateCompare = pgsd.inDate[pgsdDateIndex]
													.compareTo(dates.get(mfDateIndex));
											if (dateCompare < 0) {
												pgsdDateIndex++;
												continue;
											}
											if (dateCompare > 0) {
												mfDateIndex++;
												continue;
											}

											ccArray1.add(
													closes.get(mfDateIndex + priceDaysDiff) / closes.get(mfDateIndex));
											ccArray2.add(tsf[pgsdDateIndex - doubleBack]
													/ tsf[pgsdDateIndex - (tsfDaysDiff + doubleBack)]);

											pgsdDateIndex++;
											mfDateIndex++;
										}

										double corr = Correlation.pearsonsCorrelation(ccArray1, ccArray2);
										if (Double.isInfinite(corr))
											continue;

										synchronized (symbolsBest) {
											TopAndBottomList tab = symbolsBest.get(closeSym + "_" + priceDaysDiff);
											if (tab == null)
												tab = new TopAndBottomList();
											tab.setTop(Math.abs(corr),
													pgsd.getSymbol() + ";" + closeSym + ";" + tsfPeriod + ";"
															+ priceDaysDiff + ";" + tsfDaysDiff + ";" + doubleBack + ";"
															+ df.format(corr));
											symbolsBest.put(closeSym + "_" + priceDaysDiff, tab);
										}
									}

								}
							}
						}
					}
				}
				System.out.println("done with " + pgsd.getSymbol());
			}
		} catch (

		InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
	}

	static DecimalFormat df = new DecimalFormat("0.0000");
}