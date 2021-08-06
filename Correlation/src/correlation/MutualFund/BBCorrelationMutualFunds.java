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
import com.tictactec.ta.lib.MAType;
import com.tictactec.ta.lib.MInteger;

import correlation.Correlation;
import util.Realign;
import util.getDatabaseConnection;
import utils.TopAndBottomList;

public class BBCorrelationMutualFunds implements Runnable {

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

		BlockingQueue<String> bbQue = new ArrayBlockingQueue<String>(4);
		BBCorrelationMutualFunds corrs = new BBCorrelationMutualFunds(bbQue);

		for (int i = 0; i < 4; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}

		for (String bbSym : gsds.keySet()) {
//			if (bbSym.compareTo("c") > 0)
//				break;

			bbQue.put(bbSym);

		}

		for (int i = 0; i < 4; i++) {
			bbQue.put("<<<STOP>>>");
		}
		while (bbQue.isEmpty() == false)
			Thread.sleep(1000);

		PrintWriter pw = new PrintWriter("bbCorrelationBBoverSignal" + "_30days_" + "MutualFunds.csv");
		pw.println("abscorr;mfSym;priceDaysDiff;bbSym;optInTimePeriod;optInNbDevUp;smDaysDiff;doubleBack;corr;bbSym");

		for (String sym : symbolsBest.keySet()) {
			pw.println(sym);
			TopAndBottomList tab = symbolsBest.get(sym);
			pw.println(tab.getTop());
		}
		pw.flush();
		pw.close();

	}

	BlockingQueue<String> bbQue;

	public BBCorrelationMutualFunds(BlockingQueue<String> bbQue) {
		this.bbQue = bbQue;

	}

	double bestCorrelation = 0;

	@Override
	public void run() {

		try {
			while (true) {
				String bbSym = bbQue.take();
				if (bbSym.contains("<<<STOP>>>"))
					return;

				Core core = new Core();
				GetETFDataUsingSQL bbGSD = gsds.get(bbSym);

				System.out.println("running with " + bbSym);
				for (int optInTimePeriod = 2; optInTimePeriod <= 30; optInTimePeriod += 3)
					for (double optInNbDevUp = .75; optInNbDevUp < 2.25; optInNbDevUp += .25) {

						MInteger outBegIdx = new MInteger();
						MInteger outNBElement = new MInteger();

						double optInNbDevDn = optInNbDevUp;
						double[] outRealUpperBand = new double[bbGSD.inClose.length];
						double[] outRealMiddleBand = new double[bbGSD.inClose.length];
						double[] outRealLowerBand = new double[bbGSD.inClose.length];
						core.bbands(0, bbGSD.inClose.length - 1, bbGSD.inClose, optInTimePeriod, optInNbDevUp,
								optInNbDevDn, MAType.Ema, outBegIdx, outNBElement, outRealUpperBand, outRealMiddleBand,
								outRealLowerBand);

						Realign.realign(outRealUpperBand, outBegIdx);
						Realign.realign(outRealMiddleBand, outBegIdx);
						Realign.realign(outRealLowerBand, outBegIdx);
						for (int priceDaysDiff = 30; priceDaysDiff <= 45; priceDaysDiff += 5) {
							for (int smDaysDiff = 1; smDaysDiff <= 20; smDaysDiff += 2) {
								for (String mfSym : mfcloses.keySet()) {

									ArrayList<Double> closes = mfcloses.get(mfSym);
									ArrayList<String> dates = mfdates.get(mfSym);

									{
										for (int doubleBack = 0; doubleBack < 5; doubleBack++) {
											ArrayList<Double> ccArray1 = new ArrayList<Double>(),
													ccArray2 = new ArrayList<Double>();

											for (int mfDateIdx = 50, mfDateIndex = 50; mfDateIdx < bbGSD.inDate.length
													- priceDaysDiff & mfDateIndex < dates.size() - priceDaysDiff;) {
												int dateCompare = bbGSD.inDate[mfDateIdx]
														.compareTo(dates.get(mfDateIndex));
												if (dateCompare < 0) {
													mfDateIdx++;
													continue;
												}
												if (dateCompare > 0) {
													mfDateIndex++;
													continue;
												}

												ccArray1.add(closes.get(mfDateIndex + priceDaysDiff)
														/ closes.get(mfDateIndex));
												ccArray2.add((outRealUpperBand[mfDateIdx - (doubleBack)]
														+ outRealLowerBand[mfDateIdx - (smDaysDiff + doubleBack)]) / 2);
												mfDateIdx++;
												mfDateIndex++;
											}

											double corr = Correlation.pearsonsCorrelation(ccArray1, ccArray2);
											if (Double.isInfinite(corr))
												continue;
											double abscorr = Math.abs(corr);
											synchronized (symbolsBest) {
												TopAndBottomList tab = symbolsBest.get(mfSym + "_" + priceDaysDiff);
												if (tab == null)
													tab = new TopAndBottomList();
												tab.setTop(Math.abs(corr),
														abscorr + ";" + mfSym + ";" + priceDaysDiff + ";" + bbSym + ";"
																+ optInTimePeriod + ";" + optInNbDevUp + ";"
																+ smDaysDiff + ";" + doubleBack + ";" + corr + ";"
																+ bbSym + ";");
												symbolsBest.put(mfSym + "_" + priceDaysDiff, tab);
											}
											if (abscorr > bestCorrelation) {
												System.out.println("best correlation " + Math.abs(corr));
												bestCorrelation = abscorr;
											}
										}
									}
								}
							}
						}
					}

				System.out.println("done with " + bbSym);
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