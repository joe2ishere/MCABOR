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

public class DoubleBackMACorrelation implements Runnable {

	static public class Queue {
		String sym;
		double[] MAs;
		int period;
		MAType matype;
		String[] dates;

		@Override
		public String toString() {
			return sym + ";" + period;
		}

	}

	public static void main(String[] args) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();

		CorrelationUpdate cu = new CorrelationUpdate(conn);
		// System.exit(0);

		int threadCount = 6;
		BlockingQueue<Queue> MAQue = new ArrayBlockingQueue<Queue>(threadCount);
		DoubleBackMACorrelation corrs = new DoubleBackMACorrelation(MAQue, cu);
		corrs.loadTables();

		for (int i = 0; i < threadCount; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}

		Core core = new Core();

		for (String MASym : cu.symList.keySet()) {
			if (cu.doneList.contains(MASym))
				continue;
			if (cu.tooHigh.contains(MASym))
				continue;

			GetETFDataUsingSQL MAGSD = cu.gsds.get(MASym);
			if (cu.symList.get(MASym) < cu.entryLimit)
				continue;
			System.out.println("running " + MASym);

			MAType matypes[] = { MAType.Sma, MAType.Ema };

			for (int period = 2; period <= 23; period += 1) {
				for (MAType matype : matypes) {
					double outMA[] = new double[MAGSD.inClose.length];
					MInteger outBegIdx = new MInteger();
					MInteger outNBElement = new MInteger();

					core.movingAverage(0, MAGSD.inClose.length - 1, MAGSD.inClose, period, matype, outBegIdx,
							outNBElement, outMA);
					Queue q = new Queue();
					q.sym = MASym;
					q.dates = MAGSD.inDate;
					q.period = period;
					q.matype = matype;
					q.MAs = outMA;
					MAQue.put(q);
					// System.out.println(q.toString());
				}
			}

			corrs.dumpTable();
			corrs.updateDoneFile(MASym);
		}

		for (int tc = 0; tc < threadCount; tc++) {
			Queue qstop = new Queue();
			qstop.sym = "<<<STOP>>>";
			MAQue.put(qstop);
		}

		while (MAQue.isEmpty() == false)
			Thread.sleep(1000);
		corrs.dumpTable();
		System.exit(0);
	}

	BlockingQueue<Queue> queuedGSD;
	CorrelationUpdate cu;

	public DoubleBackMACorrelation(BlockingQueue<Queue> MAQue, CorrelationUpdate cu) {
		this.queuedGSD = MAQue;
		this.cu = cu;

	}

	File doneFile = new File("MADoubleDone.txt");
	public static File tabFile = new File("MADoubleTaB.txt");

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

	void updateDoneFile(String MASym) {

		FileWriter fw;
		try {
			fw = new FileWriter(doneFile, true);
		} catch (IOException e) {

			e.printStackTrace();
			System.exit(0);
			return;
		}
		PrintWriter pw = new PrintWriter(fw);
		pw.println(MASym);
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

						for (int functionDaysDiff = 1; functionDaysDiff <= 5; functionDaysDiff += 1) {
							{
								/*
								 * ccarray1 has to be computed here so the dates align correctly
								 */
								int MADayIndex = 50, closingDayIndex = 50;
								while (q.dates[MADayIndex].compareTo(closingGSD.inDate[closingDayIndex]) < 0)
									MADayIndex++;
								while (q.dates[MADayIndex].compareTo(closingGSD.inDate[closingDayIndex]) > 0)
									closingDayIndex++;

								int startMADay = MADayIndex;
								int startClosingDay = closingDayIndex;

								for (int doubleBack = 1; doubleBack <= 3; doubleBack += 2) {
									ArrayList<Double> ccArray1 = new ArrayList<Double>();
									ArrayList<Double> ccArray2 = new ArrayList<Double>();

									for (MADayIndex = startMADay, closingDayIndex = startClosingDay; MADayIndex < q.dates.length
											- pricefunctionDaysDiff
											& closingDayIndex < closingGSD.inDate.length - pricefunctionDaysDiff;) {
										String MADate = q.dates[MADayIndex];
										String closingDate = closingGSD.inDate[closingDayIndex];
										int dcomp = MADate.compareTo(closingDate);
										if (dcomp < 0) {
											MADayIndex++;
											continue;
										}
										if (dcomp > 0) {
											closingDayIndex++;
											continue;
										}

										if (q.dates[MADayIndex].compareTo(closingGSD.inDate[closingDayIndex]) != 0) {
											System.out.println("logic error comparing dates");
											System.exit(-2);
										}
										ccArray1.add(closingGSD.inClose[closingDayIndex + pricefunctionDaysDiff]
												/ closingGSD.inClose[closingDayIndex]);
										ccArray2.add((q.MAs[MADayIndex - (doubleBack)]
												+ q.MAs[MADayIndex - (functionDaysDiff + doubleBack)]) / 2);

										MADayIndex++;

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
												makeKey(closingSymbol, pricefunctionDaysDiff, qSymbol, q.period,
														q.matype.name(), functionDaysDiff, doubleBack, corr));
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
										System.out.println("best correlation " + Math.abs(corr));
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

	public static int getPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[3]);
	}

	public static String getMAType(String keyIn) {
		String ins[] = keyIn.split(";");
		return ins[4];
	}

	public static int getMAfunctionDaysDiff(String keyIn) {
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

	String makeKey(String symbol, int pricefunctionDaysDiff, String functionSymbol, int period, String matype,
			int functionDaysDiff, int MADaysBack, double corr) {

		return symbol + ";" + pricefunctionDaysDiff + ";" + functionSymbol + ";" + period + ";" + matype + ";"
				+ functionDaysDiff + ";" + MADaysBack + ";" + corr;
	}

	static DecimalFormat df = new DecimalFormat("0.0000");

}
