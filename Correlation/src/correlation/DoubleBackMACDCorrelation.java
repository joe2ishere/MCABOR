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
import com.tictactec.ta.lib.MInteger;

import util.Realign;
import util.getDatabaseConnection;
import utils.TopAndBottomList;

public class DoubleBackMACDCorrelation implements Runnable {

	static TreeMap<String, Integer> symList = new TreeMap<>();

	static TreeMap<String, GetETFDataUsingSQL> gsds = new TreeMap<>();

	double bestCorrelation = 0;

	static ArrayList<String> doneList = new ArrayList<>();

	TreeMap<String, TopAndBottomList> tops = new TreeMap<>();

	static public class Queue {
		String sym;
		double[] macds;
		double[] macdSignals;
		int fastPeriod;
		int slowPeriod;
		int signalPeriod;
		String[] dates;

		@Override
		public String toString() {
			return sym + ";" + fastPeriod + ";" + slowPeriod + ";" + signalPeriod;
		}

	}

	static ArrayList<String> updateSymbol = CorrelationUpdateAllOrSome.getUpdateSymbolList();;
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

		int threadCount = 3;
		BlockingQueue<Queue> macdQue = new ArrayBlockingQueue<Queue>(threadCount);
		DoubleBackMACDCorrelation corrs = new DoubleBackMACDCorrelation(macdQue);
		corrs.loadTables();

		for (int i = 0; i < threadCount; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}
		Core core = new Core();
		for (String macdSym : symList.keySet()) {
			if (doneList.contains(macdSym))
				continue;
			GetETFDataUsingSQL macdGSD = gsds.get(macdSym);
			if (symList.get(macdSym) < 2000)
				continue;
			System.out.println("running " + macdSym);
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

						Realign.realign(outMACD, outBegIdx);
						Realign.realign(outMACDSignal, outBegIdx);
						Realign.realign(outMACDHist, outBegIdx);
						Queue q = new Queue();
						q.sym = macdSym;
						q.dates = macdGSD.inDate;
						q.fastPeriod = optInFastPeriod;
						q.slowPeriod = optInSlowPeriod;
						q.signalPeriod = optInSignalPeriod;
						q.macds = outMACD;
						q.macdSignals = outMACDSignal;
						macdQue.put(q);

						// System.out.println(q.toString());
					}

				}
			}

			corrs.dumpTable();
			corrs.updateDoneFile(macdSym);
		}

		for (int tc = 0; tc < threadCount; tc++) {
			Queue qstop = new Queue();
			qstop.sym = "<<<STOP>>>";
			macdQue.put(qstop);
		}

		while (macdQue.isEmpty() == false)
			Thread.sleep(1000);
		corrs.dumpTable();

		System.exit(0);
	}

	BlockingQueue<Queue> queuedGSD;

	public DoubleBackMACDCorrelation(BlockingQueue<Queue> macdQue) {
		this.queuedGSD = macdQue;

	}

	File doneFile = new File("macdDoubleDone.txt");
	public static File tabFile = new File("macdDoubleTaB.txt");

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

					String ins[] = in.split(":");
					tab.setTop(Double.parseDouble(ins[1]), ins[2]);
				}

			}
			fr.close();

			System.out.println("end of load tables");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	void updateDoneFile(String macdSym) {

		FileWriter fw;
		try {
			fw = new FileWriter(doneFile, true);
		} catch (IOException e) {

			e.printStackTrace();
			System.exit(0);
			return;
		}
		PrintWriter pw = new PrintWriter(fw);
		pw.println(macdSym);
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
					nextDD: for (int pricefunctionDaysDiff = 1; pricefunctionDaysDiff <= 30; pricefunctionDaysDiff += 1) {

						for (int smfunctionDaysDiff = 1; smfunctionDaysDiff <= 9; smfunctionDaysDiff += 2) {
							{
								/*
								 * ccarray1 has to be computed here so the dates align correctly
								 */
								int macdDayIndex = 50, closingDayIndex = 50;
								while (q.dates[macdDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) < 0)
									macdDayIndex++;
								while (q.dates[macdDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) > 0)
									closingDayIndex++;

								int startMACDDay = macdDayIndex;
								int startClosingDay = closingDayIndex;
								for (int doubleBack = 0; doubleBack < 2; doubleBack += 1) {
									ArrayList<Double> ccArray1 = new ArrayList<Double>();
									ArrayList<Double> ccArray2 = new ArrayList<Double>();

									for (macdDayIndex = startMACDDay, closingDayIndex = startClosingDay; macdDayIndex < q.dates.length
											- pricefunctionDaysDiff
											& closingDayIndex < closingGSD.inDate.length - pricefunctionDaysDiff;) {
										String macdDate = q.dates[macdDayIndex];
										String closingDate = closingGSD.inDate[closingDayIndex];
										int dcomp = macdDate.compareTo(closingDate);
										if (dcomp < 0) {
											macdDayIndex++;
											continue;
										}
										if (dcomp > 0) {
											closingDayIndex++;
											continue;
										}

										if (q.dates[macdDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) != 0) {
											System.out.println("logic error comparing dates");
											System.exit(-2);
										}
										ccArray1.add(closingGSD.inClose[closingDayIndex + pricefunctionDaysDiff]
												/ closingGSD.inClose[closingDayIndex]);
										ccArray2.add((q.macds[macdDayIndex - (doubleBack)]
												+ q.macds[macdDayIndex - (smfunctionDaysDiff + doubleBack)]) / 2);

										macdDayIndex++;

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
										for (int i = 0; i < 5; i++)
											if ((tabBest.getTopDescription()[i].contains(";" + qSymbol)
													& tabBest.getTopValue(i) >= abscorr)) {
												continue nextDD;
											}
										{
											int setAt = tabBest.setTop(abscorr,
													makeKey(closingSymbol, pricefunctionDaysDiff, qSymbol, q.fastPeriod,
															q.slowPeriod, q.signalPeriod, smfunctionDaysDiff,
															doubleBack, corr));
//										if (setAt != -1) {
//											if (setAt > 0) {
//												for (int i = 0; i < setAt; i++) {
//													if (tabBest.getTopDescription()[i].contains(";" + qSymbol))
//														tabBest.removeFromTop(setAt);
//
//												}
//											}
//											for (int i = setAt + 1; i < tabBest.size; i++) {
//												if (tabBest.getTopDescription()[i].contains(";" + qSymbol))
//													tabBest.removeFromTop(i);
//											}
//										}
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
			// TODO Auto-generated catch block
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

	public static String getfunctionSymbol(String keyIn) {
		String ins[] = keyIn.split(";");
		return ins[2];
	}

	public static int getFast(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[3]);
	}

	public static int getSlow(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[4]);
	}

	public static int getSignal(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[5]);
	}

	public static int getMacdfunctionDaysDiff(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[6]);
	}

	public static int getDoubleBack(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[7]);
	}

	public static double getCorr(String keyIn) {
		String ins[] = keyIn.split(";");
		return Double.parseDouble(ins[8]);
	}

	String makeKey(String symbol, int pricefunctionDaysDiff, String functionSymbol, int fast, int slow, int signal,
			int smfunctionDaysDiff, int macdDaysBack, double corr) {

		return symbol + ";" + pricefunctionDaysDiff + ";" + functionSymbol + ";" + fast + ";" + slow + ";" + signal
				+ ";" + smfunctionDaysDiff + ";" + macdDaysBack + ";" + corr;
	}

	static DecimalFormat df = new DecimalFormat("0.0000");

}
