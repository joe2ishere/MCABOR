package correlation.Build;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;

import correlation.Correlation;
import util.Realign;
import util.getDatabaseConnection;
import utils.TopAndBottomList;

public class DoubleBackDMICorrelation implements Runnable {

	static public class Queue {
		String sym;
		double[] dmis;
		int dmiPeriod;
		String[] dates;

		@Override
		public String toString() {
			return sym + ";" + dmiPeriod;
		}

	}

	static boolean doingBigDayDiff = false; // if false just doing 30 days; otherwise 130 days
	// if 130 days then just a limited number of etfs are done.
	static ArrayList<String> bigDaysDiffList = new ArrayList<String>(Arrays.asList("qqq", "gld", "xle"));

	public static void main(String[] args) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();

		CorrelationUpdate cu = new CorrelationUpdate(conn);

		int threadCount = 5;
		BlockingQueue<Queue> dmiQue = new ArrayBlockingQueue<Queue>(threadCount);
		DoubleBackDMICorrelation corrs = new DoubleBackDMICorrelation(dmiQue, cu);
		corrs.loadTables();

		for (int i = 0; i < threadCount; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}
		Core core = new Core();
		for (String dmiSym : cu.symList.keySet()) {

			if (cu.doneList.contains(dmiSym))
				continue;

			GetETFDataUsingSQL dmiGSD = cu.gsds.get(dmiSym);
			if (cu.symList.get(dmiSym) < cu.entryLimit)
				continue;
			System.out.println("running " + dmiSym);
			for (int dmiPeriod = 3; dmiPeriod <= 30; dmiPeriod += 1) {
				{
					double dmi[] = new double[dmiGSD.inClose.length];
					MInteger outBegIdmi = new MInteger();
					MInteger outNBElement = new MInteger();

					core.dx(0, dmiGSD.inClose.length - 1, dmiGSD.inHigh, dmiGSD.inLow, dmiGSD.inClose, dmiPeriod,
							outBegIdmi, outNBElement, dmi);

					Realign.realign(dmi, outBegIdmi.value);
					Queue q = new Queue();
					q.sym = dmiSym;
					q.dates = dmiGSD.inDate;
					q.dmiPeriod = dmiPeriod;
					q.dmis = dmi;
					dmiQue.put(q);

				}
			}
			corrs.dumpTable();
			System.out.println(dmiSym);
			corrs.updateDoneFile(dmiSym);
		}

		for (int tc = 0; tc < threadCount; tc++) {
			Queue qstop = new Queue();
			qstop.sym = "<<<STOP>>>";
			dmiQue.put(qstop);
		}

		while (dmiQue.isEmpty() == false)
			Thread.sleep(1000);
		corrs.dumpTable();
		System.exit(0);
	}

	BlockingQueue<Queue> queuedGSD;
	CorrelationUpdate cu;

	public DoubleBackDMICorrelation(BlockingQueue<Queue> dmiQue, CorrelationUpdate cu) {
		this.queuedGSD = dmiQue;
		this.cu = cu;

	}

	File doneFile = new File("dmiDoubleDone" + (doingBigDayDiff ? "130Days" : "") + ".txt");
	static File tabFile = new File("dmiDoubleTaB" + (doingBigDayDiff ? "130Days" : "") + ".txt");

	public void loadTables() {
		if (doneFile.exists() == false)
			return;

		try {
			System.out.println("start load");
			FileReader fr = new FileReader(doneFile);
			BufferedReader br = new BufferedReader(fr);
			String in = "";
			if (cu.updateSymbol == null) {
				while ((in = br.readLine()) != null)
					cu.doneList.add(in.trim());
			}
			fr.close();
			fr = new FileReader(tabFile);
			br = new BufferedReader(fr);
			in = "";
			while ((in = br.readLine()) != null) {

				TopAndBottomList tab = cu.tops.get(in);
				if (tab == null) {
					tab = new TopAndBottomList(10);
					cu.tops.put(in, tab);
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

	void updateDoneFile(String dmiSym) {

		FileWriter fw;
		try {
			fw = new FileWriter(doneFile, true);
		} catch (IOException e) {

			e.printStackTrace();
			System.exit(0);
			return;
		}
		PrintWriter pw = new PrintWriter(fw, true);
		pw.println(dmiSym);
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

		for (String key : cu.tops.keySet()) {
			pw.println(key);
			TopAndBottomList tab = cu.tops.get(key);
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

				for (String closingSymbol : cu.symList.keySet()) {
					if (cu.updateSymbol != null)
						if (cu.updateSymbol.contains(closingSymbol) == false)
							continue;
					if (doingBigDayDiff) {
						if (bigDaysDiffList.contains(closingSymbol) == false)
							continue;
					}
					GetETFDataUsingSQL closingGSD = cu.gsds.get(closingSymbol);

					for (int pricefunctionDaysDiff = 1; pricefunctionDaysDiff <= (doingBigDayDiff ? 130
							: 30); pricefunctionDaysDiff += 1) {

						for (int functionDaysDiff = 1; functionDaysDiff <= 8; functionDaysDiff += 2) {
							{
								/*
								 * ccarray1 has to be computed here so the dates align correctly
								 */
								int dmiDayIndex = 50, closingDayIndex = 50;
								while (q.dates[dmiDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) < 0)
									dmiDayIndex++;
								while (q.dates[dmiDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) > 0)
									closingDayIndex++;

								int startDMIDay = dmiDayIndex;
								int startClosingDay = closingDayIndex;

								nextDD: for (int dmiDaysBack = 1; dmiDaysBack < 6; dmiDaysBack += 2) {
									ArrayList<Double> ccArray1 = new ArrayList<Double>();
									ArrayList<Double> ccArray2 = new ArrayList<Double>();

									for (dmiDayIndex = startDMIDay, closingDayIndex = startClosingDay; dmiDayIndex < q.dates.length
											- pricefunctionDaysDiff
											& closingDayIndex < closingGSD.inDate.length - pricefunctionDaysDiff;) {
										String dmiDate = q.dates[dmiDayIndex];
										String closingDate = closingGSD.inDate[closingDayIndex];
										int dcomp = dmiDate.compareTo(closingDate);
										if (dcomp < 0) {
											dmiDayIndex++;
											continue;
										}
										if (dcomp > 0) {
											closingDayIndex++;
											continue;
										}

										if (q.dates[dmiDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) != 0) {
											System.out.println("logic error comparing dates");
											System.exit(-2);
										}
										ccArray1.add(closingGSD.inClose[closingDayIndex + pricefunctionDaysDiff]
												/ closingGSD.inClose[closingDayIndex]);
										ccArray2.add(q.dmis[dmiDayIndex - (dmiDaysBack)]
												/ q.dmis[dmiDayIndex - (functionDaysDiff + dmiDaysBack)]);

										dmiDayIndex++;

										closingDayIndex++;

									}

									double corr = Correlation.pearsonsCorrelation(ccArray1, ccArray2);
									if (Double.isInfinite(corr))
										continue;
									if (Double.isNaN(corr))
										continue;
									double abscorr = Math.abs(corr);
									String key = closingSymbol + "_" + pricefunctionDaysDiff;
									synchronized (cu.tops) {
										TopAndBottomList tabBest = cu.tops.get(key);
										if (tabBest == null) {
											tabBest = new TopAndBottomList(10);
											cu.tops.put(key, tabBest);
										}
										for (int i = 0; i < 5; i++)
											if ((tabBest.getTopDescription()[i].contains(";" + qSymbol)
													& tabBest.getTopValue(i) >= abscorr)) {
												continue nextDD;
											}
										int setAt = tabBest.setTop(abscorr,
												makeKey(closingSymbol, pricefunctionDaysDiff, qSymbol, q.dmiPeriod,
														functionDaysDiff, dmiDaysBack, corr));
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
										if (abscorr > cu.bestCorrelation) {
											System.out.println("best correlation " + Math.abs(corr));
											cu.bestCorrelation = abscorr;
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

	public static int getDMIPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[3]);
	}

	public static int getDMIDaysBack(String keyIn) {
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

	String makeKey(String symbol, int pricefunctionDaysDiff, String functionSymbol, int dmi, int dmifunctionDaysDiff,
			int dmiDaysBack, double corr) {

		return symbol + ";" + pricefunctionDaysDiff + ";" + functionSymbol + ";" + dmi + ";" + dmifunctionDaysDiff + ";"
				+ dmiDaysBack + ";" + corr;
	}

	static DecimalFormat df = new DecimalFormat("0.0000");

}
