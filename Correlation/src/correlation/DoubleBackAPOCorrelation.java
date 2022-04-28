package correlation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MAType;
import com.tictactec.ta.lib.MInteger;

import util.getDatabaseConnection;
import utils.TopAndBottomList;

public class DoubleBackAPOCorrelation implements Runnable {

	static public class Queue {
		String sym;
		double[] apos;
		int fastPeriod;
		int slowPeriod;
		String[] dates;

		@Override
		public String toString() {
			return sym + ";" + fastPeriod + ";" + slowPeriod;
		}

	}

	public static void main(String[] args) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();

		CorrelationUpdate cu = new CorrelationUpdate(conn);
		// System.exit(0);

		int threadCount = 6;
		BlockingQueue<Queue> apoQue = new ArrayBlockingQueue<Queue>(threadCount);
		DoubleBackAPOCorrelation corrs = new DoubleBackAPOCorrelation(apoQue, cu);
		corrs.loadTables();

		for (int i = 0; i < threadCount; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}

		Core core = new Core();

		for (String apoSym : cu.symList.keySet()) {
			if (cu.doneList.contains(apoSym))
				continue;
			if (cu.tooHigh.contains(apoSym))
				continue;

			GetETFDataUsingSQL apoGSD = cu.gsds.get(apoSym);
			if (cu.symList.get(apoSym) < cu.entryLimit)
				continue;
			System.out.println("running " + apoSym);

			for (int fastPeriod = 2; fastPeriod <= 15; fastPeriod += 2) {
				for (int slowPeriod = 10; slowPeriod > fastPeriod & slowPeriod <= 30; slowPeriod += 2) {

					double outapo[] = new double[apoGSD.inClose.length];
					MInteger outBegIdx = new MInteger();
					MInteger outNBElement = new MInteger();
					core.apo(0, apoGSD.inClose.length - 1, apoGSD.inClose, fastPeriod, slowPeriod, MAType.Ema,
							outBegIdx, outNBElement, outapo);
					Queue q = new Queue();
					q.sym = apoSym;
					q.dates = apoGSD.inDate;
					q.fastPeriod = fastPeriod;
					q.slowPeriod = slowPeriod;
					q.apos = outapo;
					apoQue.put(q);

					// System.out.println(q.toString());
				}
			}

			corrs.dumpTable();
			corrs.updateDoneFile(apoSym);
		}

		for (int tc = 0; tc < threadCount; tc++) {
			Queue qstop = new Queue();
			qstop.sym = "<<<STOP>>>";
			apoQue.put(qstop);
		}

		while (apoQue.isEmpty() == false)
			Thread.sleep(1000);
		corrs.dumpTable();
		System.exit(0);
	}

	BlockingQueue<Queue> queuedGSD;
	CorrelationUpdate cu;

	public DoubleBackAPOCorrelation(BlockingQueue<Queue> apoQue, CorrelationUpdate cu) {
		this.queuedGSD = apoQue;
		this.cu = cu;

	}

	File doneFile = new File("apoDoubleDone.txt");
	public static File tabFile = new File("apoDoubleTaB.txt");

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

	void updateDoneFile(String apoSym) {

		FileWriter fw;
		try {
			fw = new FileWriter(doneFile, true);
		} catch (IOException e) {

			e.printStackTrace();
			System.exit(0);
			return;
		}
		PrintWriter pw = new PrintWriter(fw);
		pw.println(apoSym);
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
		synchronized (cu.tops) {

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
					if (closingSymbol.compareTo(qSymbol) == 0)
						continue;

					GetETFDataUsingSQL closingGSD = cu.gsds.get(closingSymbol);

					for (int pricefunctionDaysDiff = 1; pricefunctionDaysDiff <= 30; pricefunctionDaysDiff += 1) {

						for (int functionDaysDiff = 1; functionDaysDiff <= 9; functionDaysDiff += 2) {
							{
								/*
								 * ccarray1 has to be computed here so the dates align correctly
								 */
								int apoDayIndex = 50, closingDayIndex = 50;
								while (q.dates[apoDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) < 0)
									apoDayIndex++;
								while (q.dates[apoDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) > 0)
									closingDayIndex++;

								int startapoDay = apoDayIndex;
								int startClosingDay = closingDayIndex;

								for (int doubleBack = 1; doubleBack <= 5; doubleBack += 2) {
									ArrayList<Double> ccArray1 = new ArrayList<Double>();
									ArrayList<Double> ccArray2 = new ArrayList<Double>();

									for (apoDayIndex = startapoDay, closingDayIndex = startClosingDay; apoDayIndex < q.dates.length
											- pricefunctionDaysDiff
											& closingDayIndex < closingGSD.inDate.length - pricefunctionDaysDiff;) {
										String apoDate = q.dates[apoDayIndex];
										String closingDate = closingGSD.inDate[closingDayIndex];
										int dcomp = apoDate.compareTo(closingDate);
										if (dcomp < 0) {
											apoDayIndex++;
											continue;
										}
										if (dcomp > 0) {
											closingDayIndex++;
											continue;
										}

										if (q.dates[apoDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) != 0) {
											System.out.println("logic error comparing dates");
											System.exit(-2);
										}
										ccArray1.add(closingGSD.inClose[closingDayIndex + pricefunctionDaysDiff]
												- closingGSD.inClose[closingDayIndex]);
										ccArray2.add((q.apos[apoDayIndex - (doubleBack)]
												- q.apos[apoDayIndex - (functionDaysDiff + doubleBack)]));

										apoDayIndex++;

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
										int setAt = tabBest.setTop(abscorr,
												makeKey(closingSymbol, pricefunctionDaysDiff, qSymbol, q.fastPeriod,
														q.slowPeriod, functionDaysDiff, doubleBack, corr));
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

									if (abscorr > cu.bestCorrelation) {
										System.out.println("best correlation " + Math.abs(corr) + ";" + closingSymbol);
										cu.bestCorrelation = abscorr;
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

	public static int getFastPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[3]);
	}

	public static int getSlowPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[4]);
	}

	public static int getFunctionDaysDiff(String keyIn) {
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

	String makeKey(String symbol, int pricefunctionDaysDiff, String functionSymbol, int fastPeriod, int slowPeriod,
			int functionDaysDiff, int apoDaysBack, double corr) {

		return symbol + ";" + pricefunctionDaysDiff + ";" + functionSymbol + ";" + fastPeriod + ";" + slowPeriod + ";"
				+ functionDaysDiff + ";" + apoDaysBack + ";" + corr;
	}

	static DecimalFormat df = new DecimalFormat("0.0000");

}
