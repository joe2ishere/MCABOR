package correlation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

import util.Realign;
import util.getDatabaseConnection;
import utils.TopAndBottomList;

public class DoubleBackBollBandsCorrelation implements Runnable {

	static TreeMap<String, Integer> symList = new TreeMap<>();

	static TreeMap<String, GetETFDataUsingSQL> gsds = new TreeMap<>();

	double bestCorrelation = 0;

	static ArrayList<String> doneList = new ArrayList<>();

	TreeMap<String, TopAndBottomList> tops = new TreeMap<>();

	static public class Queue {
		String sym;
		double[] upper;
		double[] middle;
		double[] lower;
		int period;
		double stdDev;

		String[] dates;

		@Override
		public String toString() {
			return sym + ";" + period + ";" + stdDev;
		}

	}

	static ArrayList<String> updateSymbol = CorrelationUpdateAllOrSome.getUpdateSymbolList();
	static TreeMap<String, Integer> alreadyDoneCount = new TreeMap<>();

	public static void main(String[] args) throws Exception {

		System.out.println(updateSymbol == null ? "Updating all symbols" : "partial update");

		Connection conn = getDatabaseConnection.makeConnection();

		PreparedStatement psLastDate = conn
				.prepareStatement("select distinct txn_date from etfprices order by txn_date desc limit 1");
		ResultSet rsLastDate = psLastDate.executeQuery();
		if (rsLastDate.next() == false) {
			System.out.println("end of date error");
			System.exit(0);
		}
		String latestDate = rsLastDate.getString(1);
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
			if (pgsd.inDate[pgsd.inDate.length - 1].compareTo(latestDate) != 0) {
				System.out.println("skipping " + sym + " with end date of  " + pgsd.inDate[pgsd.inDate.length - 1]);
				continue;

			}

			gsds.put(sym, pgsd);
			symList.put(sym, cnt);

		}

		// System.exit(0);

		int threadCount = 4;
		BlockingQueue<Queue> BollBandsQue = new ArrayBlockingQueue<Queue>(threadCount);
		DoubleBackBollBandsCorrelation corrs = new DoubleBackBollBandsCorrelation(BollBandsQue);
		corrs.loadTables();

		for (int i = 0; i < threadCount; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}
		Core core = new Core();
		for (String etfSymbol : symList.keySet()) {
			if (doneList.contains(etfSymbol))
				continue;
			GetETFDataUsingSQL BollBandsGSD = gsds.get(etfSymbol);

			if (symList.get(etfSymbol) < 2000)
				continue;

			System.out.println("running " + etfSymbol);

			for (int optInTimePeriod = 3; optInTimePeriod <= 20; optInTimePeriod += 2)
				for (double optInNbDevUp = .875; optInNbDevUp < 2.25; optInNbDevUp += .25) {

					MInteger outBegIdx = new MInteger();
					MInteger outNBElement = new MInteger();

					double optInNbDevDn = optInNbDevUp;
					double[] outRealUpperBand = new double[BollBandsGSD.inClose.length];
					double[] outRealMiddleBand = new double[BollBandsGSD.inClose.length];
					double[] outRealLowerBand = new double[BollBandsGSD.inClose.length];
					core.bbands(0, BollBandsGSD.inClose.length - 1, BollBandsGSD.inClose, optInTimePeriod, optInNbDevUp,
							optInNbDevDn, MAType.Ema, outBegIdx, outNBElement, outRealUpperBand, outRealMiddleBand,
							outRealLowerBand);

					Realign.realign(outRealUpperBand, outBegIdx);
					Realign.realign(outRealMiddleBand, outBegIdx);
					Realign.realign(outRealLowerBand, outBegIdx);
					Queue q = new Queue();
					q.sym = etfSymbol;
					q.dates = BollBandsGSD.inDate;
					q.period = optInTimePeriod;
					q.stdDev = optInNbDevDn;
					q.upper = outRealUpperBand;
					q.middle = outRealMiddleBand;
					q.lower = outRealLowerBand;
					BollBandsQue.put(q);

					// System.out.println(q.toString());
				}

			corrs.dumpTable();
			corrs.updateDoneFile(etfSymbol);
		}

		for (

				int tc = 0; tc < threadCount; tc++) {
			Queue qstop = new Queue();
			qstop.sym = "<<<STOP>>>";
			BollBandsQue.put(qstop);
		}

		while (BollBandsQue.isEmpty() == false)
			Thread.sleep(1000);
		corrs.dumpTable();
		System.exit(0);
	}

	BlockingQueue<Queue> queuedGSD;

	public DoubleBackBollBandsCorrelation(BlockingQueue<Queue> BollBandsQue) {
		this.queuedGSD = BollBandsQue;

	}

	File doneFile = new File("BollBandsDoubleDone.txt");
	public static File tabFile = new File("BollBandsDoubleTaB.txt");

	public void loadTables() {

		if (doneFile.exists() == false)
			return;

		try {
			System.out.println("start load");
			FileReader fr = new FileReader(doneFile);
			BufferedReader br = new BufferedReader(fr);
			String in = "";
			if (updateSymbol == null) {
				while ((in = br.readLine()) != null)
					doneList.add(in.trim());
			}
			fr.close();
			fr = new FileReader(tabFile);
			br = new BufferedReader(fr);
			in = "";
			while ((in = br.readLine()) != null) {

				TopAndBottomList tab = tops.get(in);
				if (tab == null) {
					tab = new TopAndBottomList(10);
					tops.put(in, tab);
				}
				for (int i = 0; i < tab.size; i++) {
					in = br.readLine();
					if (in == null)
						break;
					// 0:0.16974685716495913:efu;aapl;6;22;6;1;16;0.1697
					String ins[] = in.split(":");
					if (ins.length < 2)
						break;
					tab.setTop(Double.parseDouble(ins[1]), ins[2]);
				}

			}
			fr.close();

			System.out.println("end of load tables");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	void updateDoneFile(String BollBandsSym) {

		FileWriter fw;
		try {
			fw = new FileWriter(doneFile, true);
		} catch (IOException e) {

			e.printStackTrace();
			System.exit(0);
			return;
		}
		PrintWriter pw = new PrintWriter(fw);
		pw.println(BollBandsSym);
		pw.flush();
		pw.close();

	}

	void dumpTable() {

		System.out.println("start dump");

		FileWriter fw;
		try {
			fw = new FileWriter(tabFile);
		} catch (IOException e) {

			e.printStackTrace();
			System.exit(0);
			return;
		}
		PrintWriter pw = new PrintWriter(fw);
		synchronized (tops) {

			for (String key : tops.keySet()) {
				pw.println(key);
				TopAndBottomList tab = tops.get(key);
				synchronized (tab) {
					for (int i = 0; i < tab.size; i++) {
						pw.println(i + ":" + tab.getTopValue(i) + ":" + tab.getTopDescription()[i]);
					}
					pw.flush();
				}
			}
		}

		pw.close();
		System.out.println("end of dump tables");

	}

	@Override
	public void run() {

		try {
			while (true) {
				Queue q = queuedGSD.take();
				String qSymbol = q.sym;

				if (qSymbol.compareTo("<<<STOP>>>") == 0)
					return;

				for (String closingSymbol : symList.keySet()) {
					if (updateSymbol != null)
						if (updateSymbol.contains(closingSymbol) == false)
							continue;

					GetETFDataUsingSQL closingGSD = gsds.get(closingSymbol);

					for (int pricefunctionDaysDiff = 1; pricefunctionDaysDiff <= 30; pricefunctionDaysDiff += 1) {

						for (int functionDaysDiff = 1; functionDaysDiff <= 5; functionDaysDiff += 2) {
							{
								/*
								 * ccarray1 has to be computed here so the dates align correctly
								 */
								int BollBandsDayIndex = 50, closingDayIndex = 50;
								while (q.dates[BollBandsDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) < 0)
									BollBandsDayIndex++;
								while (q.dates[BollBandsDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) > 0)
									closingDayIndex++;

								int startBollBandsDay = BollBandsDayIndex;
								int startClosingDay = closingDayIndex;

								for (int doubleBack = 0; doubleBack < 2; doubleBack += 1) {
									ArrayList<Double> ccArray1 = new ArrayList<Double>();
									ArrayList<Double> ccArray2 = new ArrayList<Double>();

									for (BollBandsDayIndex = startBollBandsDay, closingDayIndex = startClosingDay; BollBandsDayIndex < q.dates.length
											- pricefunctionDaysDiff
											& closingDayIndex < closingGSD.inDate.length - pricefunctionDaysDiff;) {
										String BollBandsDate = q.dates[BollBandsDayIndex];
										String closingDate = closingGSD.inDate[closingDayIndex];
										int dcomp = BollBandsDate.compareTo(closingDate);
										if (dcomp < 0) {
											BollBandsDayIndex++;
											continue;
										}
										if (dcomp > 0) {
											closingDayIndex++;
											continue;
										}

										if (q.dates[BollBandsDayIndex]
												.compareTo(closingGSD.inDate[closingDayIndex]) != 0) {
											System.out.println("logic error comparing dates");
											System.exit(-2);
										}
										ccArray1.add(closingGSD.inClose[closingDayIndex + pricefunctionDaysDiff]
												/ closingGSD.inClose[closingDayIndex]);
										ccArray2.add((q.upper[BollBandsDayIndex - (doubleBack)]
												+ q.lower[BollBandsDayIndex - (functionDaysDiff + doubleBack)]) / 2);

										BollBandsDayIndex++;

										closingDayIndex++;

									}

									double corr = Correlation.pearsonsCorrelation(ccArray1, ccArray2);
									if (Double.isInfinite(corr))
										continue;
									if (Double.isNaN(corr))
										continue;
									double abscorr = Math.abs(corr);
									String key = closingSymbol + "_" + pricefunctionDaysDiff;
									synchronized (tops) {

										TopAndBottomList tabBest = tops.get(key);
										if (tabBest == null) {
											tabBest = new TopAndBottomList(10);
											tops.put(key, tabBest);
										}
										int setAt = tabBest.setTop(abscorr,
												makeKey(closingSymbol, pricefunctionDaysDiff, qSymbol, q.period,
														q.stdDev, functionDaysDiff, doubleBack, corr));
										if (setAt != -1) {
											if (setAt > 0) {
												for (int i = 0; i < setAt; i++) {
													if (tabBest.getTopDescription()[i].contains(";" + qSymbol))
														tabBest.removeFromTop(setAt);

												}
											}
											for (int i = setAt + 1; i < tabBest.size; i++) {
												if (tabBest.getTopDescription()[i].contains(";" + qSymbol))
													tabBest.removeFromTop(i);
											}
										}
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

		} catch (

		InterruptedException e1) {
			e1.printStackTrace();
			return;
		}
	}

	public static String getCloseSym(String keyIn) {
		String ins[] = keyIn.split(";");
		return ins[0];
	}

	public static int getPricefunctionDaysDiff(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[1]);
	}

	public static String getBollBandsSymbol(String keyIn) {
		String ins[] = keyIn.split(";");
		return ins[2];
	}

	public static int getPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[3]);
	}

	public static double getStdDev(String keyIn) {
		String ins[] = keyIn.split(";");
		return Double.parseDouble(ins[4]);
	}

	public static int getBollBandsfunctionDaysDiff(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[5]);
	}

	public static int getDoubleBack(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[6]);
	}

	public static double getCorr(String keyIn) {
		String ins[] = keyIn.split(";");
		return Double.parseDouble(ins[7]);
	}

	String makeKey(String symbol, int pricefunctionDaysDiff, String BollBandsSymbol, int period, double stddev,
			int smfunctionDaysDiff, int BollBandsDaysBack, double corr) {

		return symbol + ";" + pricefunctionDaysDiff + ";" + BollBandsSymbol + ";" + period + ";" + stddev + ";"
				+ smfunctionDaysDiff + ";" + BollBandsDaysBack + ";" + corr;
	}

	static DecimalFormat df = new DecimalFormat("0.0000");

}
