package correlation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import util.Averager;
import util.Realign;
import util.getDatabaseConnection;

public class NATRMakeARFFfromSQL {

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

		Core core = new Core();
		Connection conn = null;

		conn = getDatabaseConnection.makeConnection();

		PreparedStatement ps = conn
				.prepareStatement("select * from natr_correlation" + " where symbol=? and toCloseDays=?");

		int daysOut = Integer.parseInt(dos);

		Averager bavg = new Averager();
		Averager savg = new Averager();

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		String startDate = gsd.inDate[50];
		ArrayList<Double> positives = new ArrayList<>();
		ArrayList<Double> negatives = new ArrayList<>();

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
		int cnt = 0;
		Collections.sort(positives);
		double highBuy = positives.get(positives.size() * 2 / 3);
		double midBuy = positives.get(positives.size() / 3);

		Collections.sort(negatives, Collections.reverseOrder());
		double lowSell = negatives.get(negatives.size() * 2 / 3);
		double midSell = negatives.get(negatives.size() / 3);

		TreeMap<String, double[]> natrs = new TreeMap<>();
		TreeMap<String, Integer> natrfunctionDaysDiff = new TreeMap<>();
		TreeMap<String, Integer> doubleBacks = new TreeMap<>();

		TreeMap<String, String[]> natrDates = new TreeMap<>();
		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		PrintWriter pw = new PrintWriter("c:/users/joe/correlationARFF/" + sym + "_natr_correlation.arff");
		pw.println("% 1. Title: " + sym + "_natr_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL gsdNATR = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(gsdNATR.inDate[50]) < 0)
				startDate = gsdNATR.inDate[50];

			System.out.println("using " + functionSymbol);
			int natrPeriod = rs.getInt("natrPeriod");

			double natr[] = new double[gsdNATR.inClose.length];
			MInteger outBegInatr = new MInteger();
			MInteger outNBElement = new MInteger();
			core.natr(0, gsdNATR.inClose.length - 1, gsdNATR.inHigh, gsdNATR.inLow, gsdNATR.inClose, natrPeriod,
					outBegInatr, outNBElement, natr);

			Realign.realign(natr, outBegInatr.value);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");

			natrs.put(symKey, natr);
			doubleBacks.put(symKey, rs.getInt("doubleBack"));
			natrfunctionDaysDiff.put(symKey, rs.getInt("natrfunctionDaysDiff"));
			pw.println("@ATTRIBUTE " + symKey + "natr NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "natrBack NUMERIC");
			natrDates.put(symKey, gsdNATR.inDate);
		}

		pw.println("@ATTRIBUTE class {StrongBuy,BUY,buy,sell,SELL,StrongSell}");

		pw.println("@DATA");

		int arraypos[] = new int[natrs.size()];
		int pos = 50;
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		eemindexLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut - 1;) {
			String posDate = gsd.inDate[iday];
			pos = 0;
			for (String key : natrs.keySet()) {
				String sdate = natrDates.get(key)[arraypos[pos]];
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

			pos = 0;
			for (String key : natrs.keySet()) {
				int giDay = iday - natrfunctionDaysDiff.get(key);
				int siDay = arraypos[pos] - natrfunctionDaysDiff.get(key);

				for (int i = 0; i < (natrfunctionDaysDiff.get(key) + daysOut); i++) {
					String gtest = gsd.inDate[giDay + i];
					String stest = natrDates.get(key)[siDay + i];
					if (gtest.compareTo(stest) != 0) {
						iday++;
						continue eemindexLoop;
					}
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

//			NATRMakeARFFfromCorrelationFile.printAttributeData(iday, daysOut, pw, natrs, natrfunctionDaysDiff, arraypos, gsd.inClose,
//					bavg, savg);
			printAttributeData(iday, daysOut, pw, natrs, natrfunctionDaysDiff, doubleBacks, arraypos, gsd.inClose,
					highBuy, midBuy, midSell, lowSell);
			iday++;
			for (pos = 0; pos < arraypos.length; pos++) {
				arraypos[pos]++;
			}

		}

		pw.close();
	}

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public static String getAttributeText(double natr[], int natrfunctionDaysDiff, int natrstart, int doubleBack) {
		return natr[natrstart - doubleBack] + "," + natr[natrstart - (natrfunctionDaysDiff + doubleBack)] + ",";

	}

	public static void printAttributeData(int iday, int daysOut, PrintWriter pw, Object inParms,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, double highBuy, double midBuy, double midSell, double lowSell) {

		TreeMap<String, double[]> natrs = (TreeMap<String, double[]>) inParms;
		int pos = 0;
		for (String key : natrs.keySet()) {

			double natr[] = natrs.get(key);
			int natrfunctionDaysDiff = functionDaysDiffMap.get(key);
			int natrstart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getAttributeText(natr, natrfunctionDaysDiff, natrstart, doubleBack.intValue()));

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

	public static void printAttributeData(int iday, int daysOut, PrintWriter pw, Object inParms,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, DeltaBands priceBands) {
		TreeMap<String, double[]> natrs = (TreeMap<String, double[]>) inParms;
		int pos = 0;
		for (String key : natrs.keySet()) {

			double natr[] = natrs.get(key);
			int natrfunctionDaysDiff = functionDaysDiffMap.get(key);
			int natrstart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getAttributeText(natr, natrfunctionDaysDiff, natrstart, doubleBack.intValue()));

			pos++;

		}
		pw.println(priceBands.getAttributeValue(iday, daysOut, closes));

	}

}
