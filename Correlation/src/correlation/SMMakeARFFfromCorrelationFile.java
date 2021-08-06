package correlation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.MAType;

import StochasticMomentum.StochasticMomentum;
import bands.DeltaBands;
import util.Averager;

public class SMMakeARFFfromCorrelationFile {

	public static void main(String[] args) throws Exception {

		LogManager.getLogManager().reset();

		Scanner sin = new Scanner(System.in);
		String sym;
		while (true) {
			System.out.print("\nSymbol: ");
			sym = sin.nextLine().trim();

			if (sym == null | sym.length() < 1) {
				sin.close();
				return;
			}
			break;
		}

		String dos;
		while (true) {
			System.out.print("\ndays out: ");
			dos = sin.nextLine().trim();

			if (dos == null | dos.length() < 1) {
				sin.close();
				return;
			}
			break;
		}
		sin.close();

		int daysOut = Integer.parseInt(dos);
		FileReader fr = new FileReader("smiTaB.txt");
		Averager bavg = new Averager();
		Averager savg = new Averager();

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		String startDate = gsd.inDate[50];
		TreeSet<Double> positives = new TreeSet<>();
		TreeSet<Double> negatives = new TreeSet<>();

		for (int i = 0; i < gsd.inClose.length - daysOut; i++) {
			double chg = gsd.inClose[i + daysOut] / gsd.inClose[i];
			if (chg > 1) {
				bavg.add(chg);
				positives.add(chg);
			} else {
				savg.add(chg);
				negatives.add(chg);

			}
		}
		double highBuy = -1;
		double midBuy = -1;
		int switchAt = positives.size() / 3;
		int cnt = 0;
		Iterator<Double> iter = positives.descendingIterator();
		while (iter.hasNext()) {
			double d = iter.next();
			cnt++;
			if (cnt > switchAt) {
				if (highBuy == -1) {
					highBuy = d;
					switchAt *= 2;
				} else {
					midBuy = d;
					break;
				}
			}

		}

		double lowSell = -1;
		double midSell = -1;
		switchAt = negatives.size() / 3;
		cnt = 0;
		iter = negatives.descendingIterator();
		while (iter.hasNext()) {
			double d = iter.next();
			cnt++;
			if (cnt > switchAt) {
				if (lowSell == -1) {
					lowSell = d;
					switchAt *= 2;
				} else {
					midSell = d;
					break;
				}
			}

		}

		TreeMap<String, StochasticMomentum> smis = new TreeMap<>();
		TreeMap<String, Integer> smfunctionDaysDiff = new TreeMap<>();
		TreeMap<String, Integer> doubleBacks = new TreeMap<>();
		TreeMap<String, String[]> smDates = new TreeMap<>();
		BufferedReader br = new BufferedReader(fr);
		String in;
		PrintWriter pw = new PrintWriter("c:/users/joe/correlationARFF/" + sym + "_smi_correlation.arff");
		pw.println("% 1. Title: " + sym + "_smi_correlation");
		pw.println("@RELATION " + sym + "_" + dos);
		boolean found = false;
		while ((in = br.readLine().trim()) != null) {
			String ins[] = in.split("_");

			if (sym.compareTo(ins[0]) == 0) {
				if (dos.compareTo(ins[1]) == 0) {
					found = true;
					break;
				}
			}
		}
		if (!found) {
			System.out.println(sym + " not found");
			return;
		}
		while ((in = br.readLine().trim()) != null) {
			if (in.length() < 1)
				break;
			if (in.contains("_"))
				break;
			String inColon[] = in.split(":");
			String ins[] = inColon[2].split(";");
			if (ins.length < 2)
				break;
			String symbol = ins[1];
//			if (symbol.contains(sym) == false)
//				continue;

			String symKey = symbol + "_" + inColon[0];
//			if (smis.containsKey(symKey))
//				continue;

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(symbol);

			if (startDate.compareTo(pgsd.inDate[50]) < 0)
				startDate = pgsd.inDate[50];

			System.out.println("using " + symbol);
			int hiLowPeriod = Integer.parseInt(ins[2]);
			int maPeriod = Integer.parseInt(ins[3]);
			int smSmoothPeriod = Integer.parseInt(ins[4]);
			int smSignalPeriod = Integer.parseInt(ins[5]);
			int functionDaysDiff = Integer.parseInt(ins[6]);
			if (functionDaysDiff != daysOut) {
				throw new Exception("days diff and out different");
			}
			StochasticMomentum sm = new StochasticMomentum(pgsd.inHigh, pgsd.inLow, pgsd.inClose, hiLowPeriod,
					MAType.Ema, maPeriod, smSmoothPeriod, smSignalPeriod);
//			sm.SMI = ScaleFrom0to1.nominalize(sm.SMI, true);
//			sm.Signal = ScaleFrom0to1.nominalize(sm.Signal, true);
			smis.put(symKey, sm);
			doubleBacks.put(symKey, Integer.parseInt(ins[7]));
			smfunctionDaysDiff.put(symKey, Integer.parseInt(ins[6]));
			pw.println("@ATTRIBUTE " + symKey + "smi NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smiRF {R,F}");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signalRF {R,F}");
			smDates.put(symKey, pgsd.inDate);
		}
		br.close();

		pw.println("@ATTRIBUTE class {StrongBuy,BUY,buy,sell,SELL,StrongSell}");

		pw.println("@DATA");

		int arraypos[] = new int[smis.size()];
		int pos = 0;
		for (String key : smis.keySet()) {
			arraypos[pos] = 0;
		}
		pos = 50;
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		eemindexLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut - 1;) {
			String posDate = gsd.inDate[iday];
//			if (posDate.startsWith("2011-09-30"))
//				System.out.println("Start");

			pos = 0;
			// if either has no dates within each period then get out;

			for (String key : smis.keySet()) {
				String sdate = smDates.get(key)[arraypos[pos]];
				int dcomp = posDate.compareTo(sdate);
				if (dcomp < 0) {
					iday++;
					continue eemindexLoop;
				}
				if (dcomp > 0) {
					arraypos[pos]++;
					continue eemindexLoop;
				}
				pos++;
			}

			Date now = sdf.parse(posDate);

			Calendar cdMove = Calendar.getInstance();
			cdMove.setTime(now);
			int idt = 1;
			int daysOutCalc = 0;
			while (true) {
				cdMove.add(Calendar.DAY_OF_MONTH, +1);
				if (cdMove.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY)
					continue;
				if (cdMove.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
					continue;
				daysOutCalc++;
				idt++;
				if (idt > daysOut)
					break;
			}
			String endDate = gsd.inDate[iday + daysOut];
			if (daysOutCalc != daysOut)
				System.out.println(posDate + " here " + endDate + " by " + daysOutCalc + " not " + daysOut);
			printAttributeData(iday, daysOut, pw, smis, smfunctionDaysDiff, arraypos, gsd.inClose, bavg, savg);
			iday++;
			for (pos = 0; pos < arraypos.length; pos++) {
				arraypos[pos]++;
			}

		}

		pw.close();
	}

	public static int daysBetween(Date d1, Date d2) {
		return (int) ((d2.getTime() - d1.getTime()) / (1000 * 60 * 60 * 24));
	}

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public static void printAttributeData(int iday, int daysOut, PrintWriter pw,
			TreeMap<String, StochasticMomentum> smis, TreeMap<String, Integer> functionDaysDiffMap, int[] arraypos,
			double closes[], Averager bavg, Averager savg) {

		int pos = 0;
		for (String key : smis.keySet()) {
			StochasticMomentum sm = smis.get(key);
			int smfunctionDaysDiff = functionDaysDiffMap.get(key);
			int smstart = arraypos[pos];
			// System.out.print(smDates.get(key)[smstart] + ";");
			pw.print(getAttributeText(sm, smfunctionDaysDiff, smstart, 0));

			pos++;

		}

		pw.flush();

	}

	public static String getAttributeText(StochasticMomentum sm, int smfunctionDaysDiff, int smstart, int doubleBack) {
		return (sm.SMI[smstart] + ","
				+ (sm.SMI[smstart] > sm.SMI[smstart - smfunctionDaysDiff - doubleBack] ? "R" : "F") + ","
				+ sm.Signal[smstart] + ","
				+ (sm.Signal[smstart] > sm.Signal[smstart - smfunctionDaysDiff - doubleBack] ? "R" : "F") + ",");

	}

	public static void printAttributeData(int iday, int daysOut, PrintWriter pw, TreeMap<String, Object> smis,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, double highBuy, double midBuy, double midSell, double lowSell) {

		int pos = 0;
		for (String key : smis.keySet()) {
			StochasticMomentum sm = (StochasticMomentum) smis.get(key);
			int smfunctionDaysDiff = functionDaysDiffMap.get(key);
			int smstart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getAttributeText(sm, smfunctionDaysDiff, smstart, doubleBack.intValue()));

			pos++;

		}
		double diff = 3.5;

		diff = closes[iday + daysOut] / closes[iday];

		if (diff > 1) {

			if (diff > highBuy)
				pw.println("StrongBuy");
			else if (diff > midBuy)
				pw.println("BUY");
			else
				pw.println("buy");
		} else {

			if (diff < lowSell)
				pw.println("StrongSell");
			else if (diff < midSell)
				pw.println("SELL");
			else
				pw.println("sell");
		}
//		pw.println(((gsd.inClose[eemi + functionDaysDiff] > gsd.inClose[eemi]) ? "buy" : "sell"));
		pw.flush();

	}

	public static void getAttributeData(int iday, int daysOut, PrintWriter pw, TreeMap<String, Object> smis,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, DeltaBands priceBands) {
		int pos = 0;
		for (String key : smis.keySet()) {
			StochasticMomentum sm = (StochasticMomentum) smis.get(key);
			int smfunctionDaysDiff = functionDaysDiffMap.get(key);
			int smstart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getAttributeText(sm, smfunctionDaysDiff, smstart, doubleBack.intValue()));

			pos++;

		}

		pw.print(priceBands.getAttributeValue(iday, daysOut, closes));
		pw.println();
//		pw.println(((gsd.inClose[eemi + functionDaysDiff] > gsd.inClose[eemi]) ? "buy" : "sell"));
		pw.flush();

	}

	public static void printAttributeData(int iday, int daysOut, PrintWriter pw, TreeMap<String, Object> smis,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, DeltaBands priceBands) {

		int pos = 0;
		String retString = priceBands.getAttributeValue(iday, daysOut, closes);
//		if (retString == null)
//			return;
		for (String key : smis.keySet()) {
			StochasticMomentum sm = (StochasticMomentum) smis.get(key);
			int smfunctionDaysDiff = functionDaysDiffMap.get(key);
			int smstart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getAttributeText(sm, smfunctionDaysDiff, smstart, doubleBack.intValue()));

			pos++;

		}
		pw.println(retString);

	}

	public static void printAttributeData(int iday, int daysOut, PrintWriter pw,
			TreeMap<String, StochasticMomentum> smis, TreeMap<String, Integer> functionDaysDiffMap, int[] arraypos,
			double[] closes, int doubleBack, DeltaBands priceBands) {
		int pos = 0;
		String retString = priceBands.getAttributeValue(iday, daysOut, closes);
//		if (retString == null)
//			return;
		for (String key : smis.keySet()) {
			StochasticMomentum sm = smis.get(key);
			int smfunctionDaysDiff = functionDaysDiffMap.get(key);
			int smstart = arraypos[pos];

			pw.print(getAttributeText(sm, smfunctionDaysDiff, smstart, doubleBack));

			pos++;

		}
		pw.println(retString);

	}

}
