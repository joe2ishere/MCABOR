package correlation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.americancoders.lineIntersect.Line;
import com.americancoders.lineIntersect.Point;

import bands.DeltaBands;
import util.HullMovingAverage;

public class HullMAMakeARFFfromCorrelationFile {

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
		FileReader fr = new FileReader("HullMADoubleTaB.txt");

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

		DeltaBands db = new DeltaBands(gsd.inClose, daysOut);
		String startDate = gsd.inDate[50];

		TreeMap<String, Object> hmas = new TreeMap<>();
		TreeMap<String, Integer> hmafunctionDaysDiff = new TreeMap<>();
		TreeMap<String, Integer> doubleBacks = new TreeMap<>();
		TreeMap<String, String[]> hmaDates = new TreeMap<>();
		BufferedReader br = new BufferedReader(fr);
		String in;
		PrintWriter pw = new PrintWriter("c:/users/joe/correlationARFF/" + sym + "_hma_correlation.arff");
		pw.println("% 1. Title: " + sym + "_hma_correlation");
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
			String symbol = ins[2];
//			if (symbol.contains(sym) == false)
//				continue;

			String symKey = symbol + "_" + inColon[0];
//			if (hmas.containsKey(symKey))
//				continue;

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(symbol);

			if (startDate.compareTo(pgsd.inDate[50]) < 0)
				startDate = pgsd.inDate[50];

			System.out.println("using " + symbol);
			int period = Integer.parseInt(ins[3]);

			int functionDaysDiff = Integer.parseInt(ins[1]);
			if (functionDaysDiff != daysOut) {
				throw new Exception("days diff and out different");
			}

			HullMovingAverage hma = new HullMovingAverage(pgsd.inClose, period);

			hmas.put(symKey, hma);
			doubleBacks.put(symKey, Integer.parseInt(ins[5]));
			hmafunctionDaysDiff.put(symKey, Integer.parseInt(ins[1]));
			pw.println("@ATTRIBUTE " + symKey + "hma NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			hmaDates.put(symKey, pgsd.inDate);
		}
		br.close();

		pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int arraypos[] = new int[hmas.size()];
		int pos = 0;
		for (String key : hmas.keySet()) {
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

			for (String key : hmas.keySet()) {
				String sdate = hmaDates.get(key)[arraypos[pos]];
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

			printAttributeData(iday, daysOut, pw, hmas, hmafunctionDaysDiff, doubleBacks, arraypos, gsd.inClose, db);
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

	public static String getAttributeText(HullMovingAverage hma, int hmafunctionDaysDiff, int hmastart, int doubleBack,
			int daysOut) {
		if (doubleBack == 0)
			doubleBack = 1;

		Point p1 = new Point(hmastart, hma.getHullMovingAverage()[hmastart]);
		Point p2 = new Point(hmastart - daysOut, hma.getHullMovingAverage()[hmastart - daysOut]);
		Line line = new Line(p1, p2);
		double newPt = (hmastart + daysOut) * line.slope + line.yintersect;
		p2 = new Point(hmastart - daysOut, hma.getHullMovingAverage()[hmastart - daysOut - doubleBack]);
		line = new Line(p1, p2);
		double newPt2 = (hmastart + daysOut) * line.slope + line.yintersect;
		return newPt2 + "," + newPt + ",";

	}

	public static void getAttributeData(int iday, int daysOut, PrintWriter pw, TreeMap<String, Object> hmas,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, DeltaBands priceBands) {
		int pos = 0;
		for (String key : hmas.keySet()) {
			HullMovingAverage hma = (HullMovingAverage) hmas.get(key);
			int hmafunctionDaysDiff = functionDaysDiffMap.get(key);
			int hmastart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getAttributeText(hma, hmafunctionDaysDiff, hmastart, doubleBack.intValue(), daysOut));

			pos++;

		}

		pw.print(priceBands.getAttributeValue(iday, daysOut, closes));
		pw.println();
//		pw.println(((gsd.inClose[eemi + functionDaysDiff] > gsd.inClose[eemi]) ? "buy" : "sell"));
		pw.flush();

	}

	public static void printAttributeData(int iday, int daysOut, PrintWriter pw, TreeMap<String, Object> hmas,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, DeltaBands priceBands) {

		int pos = 0;
		String retString = priceBands.getAttributeValue(iday, daysOut, closes);
//		if (retString == null)
//			return;
		for (String key : hmas.keySet()) {
			HullMovingAverage hma = (HullMovingAverage) hmas.get(key);
			int hmafunctionDaysDiff = functionDaysDiffMap.get(key);
			int hmastart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getAttributeText(hma, hmafunctionDaysDiff, hmastart, doubleBack.intValue(), daysOut));

			pos++;

		}
		pw.println(retString);

	}

	public static void printAttributeData(int iday, int daysOut, PrintWriter pw,
			TreeMap<String, HullMovingAverage> hmas, TreeMap<String, Integer> functionDaysDiffMap, int[] arraypos,
			double[] closes, int doubleBack, DeltaBands priceBands) {
		int pos = 0;
		String retString = priceBands.getAttributeValue(iday, daysOut, closes);
//		if (retString == null)
//			return;
		for (String key : hmas.keySet()) {
			HullMovingAverage hma = hmas.get(key);
			int hmafunctionDaysDiff = functionDaysDiffMap.get(key);
			int hmastart = arraypos[pos];

			pw.print(getAttributeText(hma, hmafunctionDaysDiff, hmastart, doubleBack, daysOut));

			pos++;

		}
		pw.println(retString);

	}

}
