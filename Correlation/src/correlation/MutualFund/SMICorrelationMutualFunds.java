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
import com.tictactec.ta.lib.MAType;

import StochasticMomentum.StochasticMomentum;
import correlation.Correlation;
import util.getDatabaseConnection;
import utils.TopAndBottomList;

public class SMICorrelationMutualFunds implements Runnable {

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

			if (vol < 10000) {
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

		PreparedStatement psMF = conn.prepareStatement("select distinct name from fidelitymutualfunds");
		PreparedStatement psMFGet = conn
				.prepareStatement("select dt, close from fidelitymutualfunds where name = ? order by dt");
		ResultSet rsMF = psMF.executeQuery();

		while (rsMF.next()) {
			String mfName = rsMF.getString(1);
			psMFGet.setString(1, mfName);
			ArrayList<Double> closes = new ArrayList<>();
			ArrayList<String> dates = new ArrayList<>();
			mfcloses.put(mfName, closes);
			mfdates.put(mfName, dates);
			ResultSet rsMFGet = psMFGet.executeQuery();
			while (rsMFGet.next()) {
				closes.add((double) rsMFGet.getFloat(2));
				dates.add(rsMFGet.getString(1));
			}

		}

		BlockingQueue<String> smiQue = new ArrayBlockingQueue<String>(4);
		SMICorrelationMutualFunds corrs = new SMICorrelationMutualFunds(smiQue);

		for (int i = 0; i < 4; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}

		for (String smiSym : gsds.keySet()) {
//			if (smiSym.compareTo("c") > 0)
//				break;

			smiQue.put(smiSym);

		}

		for (int i = 0; i < 4; i++) {
			smiQue.put("<<<STOP>>>");
		}
		while (smiQue.isEmpty() == false)
			Thread.sleep(1000);

		PrintWriter pw = new PrintWriter("smiCorrelationSMIoverSignal" + "_30days_" + "MutualFunds.csv");
		pw.println(
				"abs Corr;smi Sym;close Sym;hlperiod;ma period; sm Period;sm Smooth;to Close Days Diff;sm Days Diff;double Back; true Corr");

		for (String sym : symbolsBest.keySet()) {
			pw.println(sym);
			TopAndBottomList tab = symbolsBest.get(sym);
			pw.println(tab.getTop());
		}
		pw.flush();
		pw.close();

	}

	BlockingQueue<String> smiQue;

	public SMICorrelationMutualFunds(BlockingQueue<String> smiQue) {
		this.smiQue = smiQue;

	}

	@Override
	public void run() {

		try {
			while (true) {
				String smiSym = smiQue.take();
				if (smiSym.contains("<<<STOP>>>"))
					return;

				GetETFDataUsingSQL pgsd = gsds.get(smiSym);

				System.out.println("running with " + pgsd.getSymbol());
				for (int hiLowPeriod = 2; hiLowPeriod <= 20; hiLowPeriod += 2) {
					if (hiLowPeriod > 10)
						hiLowPeriod++;
					// for (int maPeriod = 2; maPeriod <= 10; maPeriod += 2)
					{
						int maPeriod = hiLowPeriod;
						for (int smSmoothPeriod = 2; smSmoothPeriod <= 20; smSmoothPeriod += 2) {
							if (smSmoothPeriod > 10)
								smSmoothPeriod++;
							// for (int smSignalPeriod = 2; smSignalPeriod <= 10; smSignalPeriod += 2)
							int smSignalPeriod = smSmoothPeriod;
							{
								StochasticMomentum sm;

								sm = new StochasticMomentum(pgsd.inHigh, pgsd.inLow, pgsd.inClose, hiLowPeriod,
										MAType.Ema, maPeriod, smSmoothPeriod, smSignalPeriod);
								for (int priceDaysDiff = 30; priceDaysDiff <= 45; priceDaysDiff += 5) {
									for (int smDaysDiff = 1; smDaysDiff <= 20; smDaysDiff += 2) {
										for (String closeSym : mfcloses.keySet()) {

											ArrayList<Double> closes = mfcloses.get(closeSym);
											ArrayList<String> dates = mfdates.get(closeSym);

											{
												for (int doubleBack = 0; doubleBack < 5; doubleBack++) {
													ArrayList<Double> ccArray1 = new ArrayList<Double>(),
															ccArray2 = new ArrayList<Double>();

													for (int pgsdDateIndex = 50, mfDateIndex = 50; pgsdDateIndex < pgsd.inDate.length
															- priceDaysDiff
															& mfDateIndex < dates.size() - priceDaysDiff;) {
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

														ccArray1.add(closes.get(mfDateIndex + priceDaysDiff)
																/ closes.get(mfDateIndex));
														ccArray2.add(sm.SMI[pgsdDateIndex - doubleBack]
																/ sm.SMI[pgsdDateIndex - (smDaysDiff + doubleBack)]);

														pgsdDateIndex++;
														mfDateIndex++;
													}

													double corr = Correlation.pearsonsCorrelation(ccArray1, ccArray2);
													if (Double.isInfinite(corr))
														continue;

													synchronized (symbolsBest) {
														TopAndBottomList tab = symbolsBest
																.get(closeSym + "_" + priceDaysDiff);
														if (tab == null)
															tab = new TopAndBottomList();
														tab.setTop(Math.abs(corr),
																pgsd.getSymbol() + ";" + closeSym + ";" + hiLowPeriod
																		+ ";" + maPeriod + ";" + smSmoothPeriod + ";"
																		+ smSignalPeriod + ";" + priceDaysDiff + ";"
																		+ smDaysDiff + ";" + doubleBack + ";"
																		+ df.format(corr));
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
				}
				System.out.println("done with " + pgsd.getSymbol());
			}
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
	}

	static DecimalFormat df = new DecimalFormat("0.0000");
}