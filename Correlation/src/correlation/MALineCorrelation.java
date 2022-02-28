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
import com.americancoders.lineIntersect.Line;
import com.tictactec.ta.lib.MAType;

import movingAvgAndLines.MovingAvgAndLineIntercept;
import util.getDatabaseConnection;
import utils.TopAndBottomList;

public class MALineCorrelation implements Runnable {

	static public class Queue {
		String sym;

		MovingAvgAndLineIntercept maali;
		int MAPeriod;
		MAType type;
		GetETFDataUsingSQL gsd;

		@Override
		public String toString() {
			return sym + ";" + MAPeriod;
		}

	}

	public static void main(String[] args) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();

		CorrelationUpdate cu = new CorrelationUpdate(conn);

		// System.exit(0);

		int threadCount = 4;
		BlockingQueue<Queue> MAQue = new ArrayBlockingQueue<Queue>(threadCount);
		MALineCorrelation corrs = new MALineCorrelation(MAQue, cu);
		corrs.loadTables();

		for (int i = 0; i < threadCount; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}

		MAType[] types = { MAType.Ema, MAType.Dema, MAType.Tema };
		for (String MASym : cu.symList.keySet()) {

			GetETFDataUsingSQL gsdMA = cu.gsds.get(MASym);
			if (cu.symList.get(MASym) < cu.entryLimit)
				continue;
			if (cu.doneList.contains(MASym))
				continue;
//			if (cu.tooHigh.contains(MASym))
//				continue;

			System.out.println("running " + MASym);
			for (int maPeriod = 2; maPeriod <= 21; maPeriod += 2) {
				for (MAType type : types) {

					MovingAvgAndLineIntercept mal = new MovingAvgAndLineIntercept(gsdMA, maPeriod, type, maPeriod,
							type);

					Queue q = new Queue();
					q.sym = MASym;
					q.MAPeriod = maPeriod;
					q.type = type;
					q.maali = mal;
					q.gsd = gsdMA;
					MAQue.put(q);
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

	public MALineCorrelation(BlockingQueue<Queue> MAQue, CorrelationUpdate cu) {
		this.queuedGSD = MAQue;
		this.cu = cu;

	}

	File doneFile = new File("MALineCorrelationDone.txt");
	public static File tabFile = new File("MALineCorrelationTab.txt");

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
					String ins[] = in.split(":");
					tab.setTop(Double.parseDouble(ins[1]), ins[2]);
				}
			}
			fr.close();

			System.out.println("end of load tables");
		} catch (Exception e) {
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
		PrintWriter pw = new PrintWriter(fw, true);
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
					GetETFDataUsingSQL closingGSD = cu.gsds.get(closingSymbol);

					for (int pricefunctionDaysDiff = 1; pricefunctionDaysDiff <= 30; pricefunctionDaysDiff += 1) {
						/*
						 * ccarray1 has to be computed here so the dates align correctly
						 */
						int maDayIndex = 100, closingDayIndex = 100;
						while (q.gsd.inDate[maDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) < 0)
							maDayIndex++;
						while (q.gsd.inDate[maDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) > 0)
							closingDayIndex++;

						int startMADay = maDayIndex;
						int startClosingDay = closingDayIndex;

						ArrayList<Double> ccArray1 = new ArrayList<Double>();
						ArrayList<Double> ccArray2 = new ArrayList<Double>();

						for (maDayIndex = startMADay, closingDayIndex = startClosingDay; maDayIndex < q.gsd.inDate.length
								- pricefunctionDaysDiff
								& closingDayIndex < closingGSD.inDate.length - pricefunctionDaysDiff;) {
							String MADate = q.gsd.inDate[maDayIndex];
							String closingDate = closingGSD.inDate[closingDayIndex];
							int dcomp = MADate.compareTo(closingDate);
							if (dcomp < 0) {
								maDayIndex++;
								continue;
							}
							if (dcomp > 0) {
								closingDayIndex++;
								continue;
							}

							if (q.gsd.inDate[maDayIndex].compareTo(closingGSD.inDate[closingDayIndex]) != 0) {
								System.out.println("logic error comparing dates");
								System.exit(-2);
							}
							Line ln, ln2, ln3;
							try {
								ln = q.maali.getCurrentLineIntercept(closingGSD.inDate[closingDayIndex], 1);
								ln2 = q.maali.getCurrentLineIntercept(closingGSD.inDate[closingDayIndex], 2);
								ln3 = q.maali.getCurrentLineIntercept(closingGSD.inDate[closingDayIndex], 3);
								if (ln == null | ln2 == null | ln3 == null) {
									maDayIndex++;
									closingDayIndex++;
									continue;
								}
							} catch (Exception e) {
								maDayIndex++;
								closingDayIndex++;
								continue;
							}
							double yln = q.gsd.inClose[maDayIndex + pricefunctionDaysDiff] * ln.slope + ln.yintersect;
							double yln2 = q.gsd.inClose[maDayIndex + pricefunctionDaysDiff] * ln2.slope
									+ ln2.yintersect;
							double yln3 = q.gsd.inClose[maDayIndex + pricefunctionDaysDiff] * ln3.slope
									+ ln3.yintersect;
							ccArray1.add(closingGSD.inClose[closingDayIndex + pricefunctionDaysDiff]
									/ closingGSD.inClose[closingDayIndex]);
							ccArray2.add(((yln + yln2 + yln3) / 3) / q.gsd.inClose[maDayIndex]);
							maDayIndex++;
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

							int setAt = tabBest.setTop(abscorr, makeKey(closingSymbol, pricefunctionDaysDiff, qSymbol,
									q.MAPeriod, q.type.name(), corr));
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

	public static int getMAPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[3]);
	}

	public static String getMaType(String keyIn) {
		String ins[] = keyIn.split(";");
		return (ins[4]);
	}

	public static double getCorr(String keyIn) {
		String ins[] = keyIn.split(";");
		return Double.parseDouble(ins[5]);
	}

	String makeKey(String symbol, int pricefunctionDaysDiff, String functionSymbol, int maPeriod, String maType,
			double corr) {

		return symbol + ";" + pricefunctionDaysDiff + ";" + functionSymbol + ";" + maPeriod + ";" + maType + ";" + corr;
	}

	static DecimalFormat df = new DecimalFormat("0.0000");

}
