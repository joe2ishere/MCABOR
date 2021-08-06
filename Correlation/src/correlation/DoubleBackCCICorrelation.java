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

public class DoubleBackCCICorrelation implements Runnable {

	static TreeMap<String, Integer> symList = new TreeMap<>();

	static TreeMap<String, GetETFDataUsingSQL> gsds = new TreeMap<>();

	double bestCorrelation = 0;

	static ArrayList<String> doneList = new ArrayList<>();

	TreeMap<String, TopAndBottomList> tops = new TreeMap<>();

	static public class Queue {
		String sym;
		double[] ccis;
		int cciPeriod;
		String[] dates;

		@Override
		public String toString() {
			return sym + ";" + cciPeriod;
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

		int threadCount = 5;
		BlockingQueue<Queue> cciQue = new ArrayBlockingQueue<Queue>(threadCount);
		DoubleBackCCICorrelation corrs = new DoubleBackCCICorrelation(cciQue);
		corrs.loadTables();

		for (int i = 0; i < threadCount; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}
		Core core = new Core();
		for (String cciSym : symList.keySet()) {

			if (doneList.contains(cciSym))
				continue;
			GetETFDataUsingSQL cciGSD = gsds.get(cciSym);
			if (symList.get(cciSym) < 2000)
				continue;
			System.out.println("running " + cciSym);
			for (int cciPeriod = 3; cciPeriod <= 30; cciPeriod += 1) {
				{

					double cci[] = new double[cciGSD.inClose.length];
					MInteger outBegIcci = new MInteger();
					MInteger outNBElement = new MInteger();

					core.cci(0, cciGSD.inClose.length - 1, cciGSD.inHigh, cciGSD.inLow, cciGSD.inClose, cciPeriod,
							outBegIcci, outNBElement, cci);

					Realign.realign(cci, outBegIcci.value);
					Queue q = new Queue();
					q.sym = cciSym;
					q.dates = cciGSD.inDate;
					q.cciPeriod = cciPeriod;
					q.ccis = cci;
					cciQue.put(q);

				}
			}
			corrs.dumpTable();
			System.out.println(cciSym);
			corrs.updateDoneFile(cciSym);
		}

		for (int tc = 0; tc < threadCount; tc++) {
			Queue qstop = new Queue();
			qstop.sym = "<<<STOP>>>";
			cciQue.put(qstop);
		}

		while (cciQue.isEmpty() == false)
			Thread.sleep(1000);
		corrs.dumpTable();
		System.exit(0);
	}

	BlockingQueue<Queue> queuedGSD;

	public DoubleBackCCICorrelation(BlockingQueue<Queue> cciQue) {
		this.queuedGSD = cciQue;

	}

	File doneFile = new File("cciDoubleDone.txt");
	static File tabFile = new File("cciDoubleTaB.txt");

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

	void updateDoneFile(String cciSym) {

		FileWriter fw;
		try {
			fw = new FileWriter(doneFile, true);
		} catch (IOException e) {

			e.printStackTrace();
			System.exit(0);
			return;
		}
		PrintWriter pw = new PrintWriter(fw, true);
		pw.println(cciSym);
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

						for (int ccifunctionDaysDiff = 1; ccifunctionDaysDiff <= 6; ccifunctionDaysDiff += 1) {
							{
								/*
								 * ccarray1 has to be computed here so the dates align correctly
								 */
								int cciDayIndex = 50, closingDayIndex = 50;
								while (q.dates[cciDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) < 0)
									cciDayIndex++;
								while (q.dates[cciDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) > 0)
									closingDayIndex++;

								int startCCIDay = cciDayIndex;
								int startClosingDay = closingDayIndex;

								nextDD: for (int cciDaysBack = 0; cciDaysBack < 2; cciDaysBack += 1) {
									ArrayList<Double> ccArray1 = new ArrayList<Double>();
									ArrayList<Double> ccArray2 = new ArrayList<Double>();

									for (cciDayIndex = startCCIDay, closingDayIndex = startClosingDay; cciDayIndex < q.dates.length
											- pricefunctionDaysDiff
											& closingDayIndex < closingGSD.inDate.length - pricefunctionDaysDiff;) {
										String cciDate = q.dates[cciDayIndex];
										String closingDate = closingGSD.inDate[closingDayIndex];
										int dcomp = cciDate.compareTo(closingDate);
										if (dcomp < 0) {
											cciDayIndex++;
											continue;
										}
										if (dcomp > 0) {
											closingDayIndex++;
											continue;
										}

										if (q.dates[cciDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) != 0) {
											System.out.println("logic error comparing dates");
											System.exit(-2);
										}
										ccArray1.add(closingGSD.inClose[closingDayIndex + pricefunctionDaysDiff]
												/ closingGSD.inClose[closingDayIndex]);
										ccArray2.add(q.ccis[cciDayIndex - (cciDaysBack)]
												/ q.ccis[cciDayIndex - (ccifunctionDaysDiff + cciDaysBack)]);

										cciDayIndex++;

										closingDayIndex++;

									}

									double corr = Correlation.pearsonsCorrelation(ccArray1, ccArray2);
									if (Double.isInfinite(corr))
										continue;
									if (Double.isNaN(corr))
										continue;
									double abscorr = Math.abs(corr);
									if (abscorr > bestCorrelation) {
										bestCorrelation = abscorr;
										System.out.println("best corr so far " + bestCorrelation);
									}
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
										int setAt = tabBest.setTop(abscorr,
												makeKey(closingSymbol, pricefunctionDaysDiff, qSymbol, q.cciPeriod,
														ccifunctionDaysDiff, cciDaysBack, corr));
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

	public static int getfunctionDaysDiff(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[1]);
	}

	public static String getfunctionSymbol(String keyIn) {
		String ins[] = keyIn.split(";");
		return ins[2];
	}

	public static int getPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[3]);
	}

	public static int getCCIDaysBack(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[4]);
	}

	public static int getDoubleBack(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[5]);
	}

	public static double getCorr(String keyIn) {
		String ins[] = keyIn.split(";");
		return Double.parseDouble(ins[6]);
	}

	String makeKey(String symbol, int pricefunctionDaysDiff, String functionSymbol, int cci, int ccifunctionDaysDiff,
			int cciDaysBack, double corr) {

		return symbol + ";" + pricefunctionDaysDiff + ";" + functionSymbol + ";" + cci + ";" + ccifunctionDaysDiff + ";"
				+ cciDaysBack + ";" + corr;
	}

	static DecimalFormat df = new DecimalFormat("0.0000");

}
