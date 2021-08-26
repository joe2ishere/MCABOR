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
import com.tictactec.ta.lib.MAType;

import StochasticMomentum.StochasticMomentum;
import util.getDatabaseConnection;
import utils.TopAndBottomList;

public class DoubleBackSMICorrelation implements Runnable {

	static TreeMap<String, Integer> symList = new TreeMap<>();

	static TreeMap<String, GetETFDataUsingSQL> gsds = new TreeMap<>();

	double bestCorrelation = 0;

	static ArrayList<String> doneList = new ArrayList<>();

	TreeMap<String, TopAndBottomList> tops = new TreeMap<>();

	static public class Queue {
		String sym;
		double[] smis;
		int hilowPeriod;
		int smoothPeriod;
		String[] dates;
		public int maPeriod;
		public int singnalPeriod;
	}

	static ArrayList<String> updateSymbol = CorrelationUpdateAllOrSome.getUpdateSymbolList();;
	static PreparedStatement countDone;
	static TreeMap<String, Integer> alreadyDoneCount = new TreeMap<>();

	public static void main(String[] args) throws Exception {
		System.out.println((updateSymbol == null) ? "Updating all symbols" : "partial update");
		Connection conn = getDatabaseConnection.makeConnection();
		countDone = conn.prepareStatement("select count(*) from sm_correlation where symbol = ? and toCloseDays = ?");
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

		int threadCount = 5;
		BlockingQueue<Queue> smiQue = new ArrayBlockingQueue<Queue>(threadCount);
		DoubleBackSMICorrelation corrs = new DoubleBackSMICorrelation(smiQue);
		corrs.loadTables();

		for (int i = 0; i < threadCount; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}

		for (String smiSym : symList.keySet()) {

			if (doneList.contains(smiSym))
				continue;

			GetETFDataUsingSQL smiGSD = gsds.get(smiSym);
			if (symList.get(smiSym) < 2000)
				continue;
			System.out.println("running " + smiSym);
			for (int hiLowPeriod = 3; hiLowPeriod <= 21; hiLowPeriod += 3) {
				for (int maPeriod = 2; maPeriod <= 4; maPeriod += 1) {
					for (int smSmoothPeriod = 2; smSmoothPeriod <= 10; smSmoothPeriod += 2) {
						for (int smSignalPeriod = 2; smSignalPeriod <= 4; smSignalPeriod += 1) {
							StochasticMomentum sm = new StochasticMomentum(smiGSD.inHigh, smiGSD.inLow, smiGSD.inClose,
									hiLowPeriod, MAType.Ema, maPeriod, smSmoothPeriod, smSignalPeriod);
							Queue q = new Queue();
							q.sym = smiSym;
							q.dates = smiGSD.inDate;
							q.hilowPeriod = hiLowPeriod;
							q.maPeriod = maPeriod;
							q.singnalPeriod = smSignalPeriod;
							q.smoothPeriod = smSmoothPeriod;
							q.smis = sm.SMI;
							smiQue.put(q);

						}
					}
				}
			}
			corrs.dumpTable();
			corrs.updateDoneFile(smiSym);
		}

		for (int tc = 0; tc < threadCount; tc++) {
			Queue qstop = new Queue();
			qstop.sym = "<<<STOP>>>";
			smiQue.put(qstop);
		}

		while (smiQue.isEmpty() == false)
			Thread.sleep(1000);
		corrs.dumpTable();
		System.exit(0);
	}

	BlockingQueue<Queue> queuedGSD;

	public DoubleBackSMICorrelation(BlockingQueue<Queue> smiQue) {
		this.queuedGSD = smiQue;

	}

	File doneFile = new File("smiDoubleDone.txt");
	static File tabFile = new File("smiDoubleTaB.txt");

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
					// 0:0.16974685716495913:efu;aapl;6;22;6;1;16;0.1697
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

	void updateDoneFile(String smiSym) {

		FileWriter fw;
		try {
			fw = new FileWriter(doneFile, true);
		} catch (IOException e) {

			e.printStackTrace();
			System.exit(0);
			return;
		}
		PrintWriter pw = new PrintWriter(fw, true);
		pw.println(smiSym);
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

						for (int smifunctionDaysDiff = 1; smifunctionDaysDiff <= 5; smifunctionDaysDiff += 2) {
							{
								/*
								 * ccarray1 has to be computed here so the dates align correctly
								 */
								int smiDayIndex = 50, closingDayIndex = 50;
								while (q.dates[smiDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) < 0)
									smiDayIndex++;
								while (q.dates[smiDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) > 0)
									closingDayIndex++;

								int startSMIDay = smiDayIndex;
								int startClosingDay = closingDayIndex;
								nextDD: for (int smiDaysBack = 0; smiDaysBack < 3; smiDaysBack += 1) {
									ArrayList<Double> ccArray1 = new ArrayList<Double>();
									ArrayList<Double> ccArray2 = new ArrayList<Double>();

									for (smiDayIndex = startSMIDay, closingDayIndex = startClosingDay; smiDayIndex < q.dates.length
											- pricefunctionDaysDiff
											& closingDayIndex < closingGSD.inDate.length - pricefunctionDaysDiff;) {
										String smiDate = q.dates[smiDayIndex];
										String closingDate = closingGSD.inDate[closingDayIndex];
										int dcomp = smiDate.compareTo(closingDate);
										if (dcomp < 0) {
											smiDayIndex++;
											continue;
										}
										if (dcomp > 0) {
											closingDayIndex++;
											continue;
										}

										if (q.dates[smiDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) != 0) {
											System.out.println("logic error comparing dates");
											System.exit(-2);
										}
										ccArray1.add(closingGSD.inClose[closingDayIndex + pricefunctionDaysDiff]
												/ closingGSD.inClose[closingDayIndex]);
										ccArray2.add((q.smis[smiDayIndex - (smiDaysBack)]
												+ q.smis[smiDayIndex - (smifunctionDaysDiff + smiDaysBack)]) / 2);

										smiDayIndex++;

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
										//int setAt = 
										tabBest.setTop(abscorr,
												makeKey(closingSymbol, pricefunctionDaysDiff, qSymbol, q.hilowPeriod,
														q.maPeriod, q.smoothPeriod, q.singnalPeriod,
														smifunctionDaysDiff, smiDaysBack, corr));

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

	public static int gethiLowPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[3]);
	}

	public static int getmaPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[4]);
	}

	public static int getsmSmoothPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[5]);
	}

	public static int getsmSignalPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[6]);
	}

	public static int getsmifunctionDaysDiff(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[7]);
	}

	public static int getDoubleBack(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[8]);
	}

	public static double getcorr(String keyIn) {
		String ins[] = keyIn.split(";");
		return Double.parseDouble(ins[9]);
	}

	String makeKey(String symbol, int pricefunctionDaysDiff, String functionSymbol, int hiLowPeriod, int maPeriod,
			int smSmoothPeriod, int smSignalPeriod, int smifunctionDaysDiff, int doubleBack, double corr) {

		return symbol + ";" + pricefunctionDaysDiff + ";" + functionSymbol + ";" + hiLowPeriod + ";" + maPeriod + ";"
				+ smSmoothPeriod + ";" + smSignalPeriod + ";" + smifunctionDaysDiff + ";" + doubleBack + ";" + corr;
	}

	static DecimalFormat df = new DecimalFormat("0.0000");

}
