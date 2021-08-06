package correlation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import util.Realign;
import util.getDatabaseConnection;

public class MACDMakeARFFfromSQL {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5

	public MACDMakeARFFfromSQL(boolean b) {
		withAttributePosition = b;
	}

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
		MACDMakeARFFfromSQL macd = new MACDMakeARFFfromSQL(false);
		macd.makeARFFFromSQL(sym, dos);
	}

	public String getFilename(String sym, String dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_macd_correlation.arff";
	}

	public TreeMap<String, Integer> dateAttribute = new TreeMap<>();

	public void makeARFFFromSQL(String sym, String dos) throws Exception {

		Core core = new Core();
		Connection conn = null;

		conn = getDatabaseConnection.makeConnection();

		PreparedStatement ps = conn
				.prepareStatement("select * from macd_correlation" + " where symbol=? and toCloseDays=?");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		String startDate = gsd.inDate[50];

		TreeMap<String, Object> macds = new TreeMap<>();
		TreeMap<String, Integer> macdfunctionDaysDiff = new TreeMap<>();
		TreeMap<String, Integer> doubleBacks = new TreeMap<>();

		TreeMap<String, String[]> macdDates = new TreeMap<>();
		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		PrintWriter pw = new PrintWriter(getFilename(sym, dos));
		pw.println("% 1. Title: " + sym + "_macd_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(pgsd.inDate[50]) < 0)
				startDate = pgsd.inDate[50];

			int fastPeriod = rs.getInt("fastPeriod");
			int slowPeriod = rs.getInt("slowPeriod");
			int signalPeriod = rs.getInt("signalPeriod");
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			double outMACD[] = new double[pgsd.inClose.length];
			double[] outMACDSignal = new double[pgsd.inClose.length];
			double[] outMACDHist = new double[pgsd.inClose.length];
			core.macd(0, pgsd.inClose.length - 1, pgsd.inClose, fastPeriod, slowPeriod, signalPeriod, outBegIdx,
					outNBElement, outMACD, outMACDSignal, outMACDHist);

			Realign.realign(outMACD, outBegIdx);
			Realign.realign(outMACDSignal, outBegIdx);
			Realign.realign(outMACDHist, outBegIdx);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");
			ArrayList<double[]> macdData = new ArrayList<>();
			macdData.add(outMACD);
			macdData.add(outMACDSignal);

			macds.put(symKey, macdData);
			macdfunctionDaysDiff.put(symKey, rs.getInt("functionDaysDiff"));
			doubleBacks.put(symKey, rs.getInt("doubleBack"));
			pw.println("@ATTRIBUTE " + symKey + "macd NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			macdDates.put(symKey, pgsd.inDate);
		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int arraypos[] = new int[macds.size()];
		int pos = 0;
		for (String key : macds.keySet()) {
			arraypos[pos] = 0;
		}
		pos = 50;
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		int attributePos = -1;
		eemindexLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut - 1;) {
			String posDate = gsd.inDate[iday];
			pos = 0;
			for (String key : macds.keySet()) {
				String sdate = macdDates.get(key)[arraypos[pos]];
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
			for (String key : macds.keySet()) {
				int giDay = iday - macdfunctionDaysDiff.get(key);
				int siDay = arraypos[pos] - macdfunctionDaysDiff.get(key);

				for (int i = 0; i < (macdfunctionDaysDiff.get(key) + daysOut); i++) {
					String gtest = gsd.inDate[giDay + i];
					String stest = macdDates.get(key)[siDay + i];
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

//			MACDMakeARFFfromCorrelationFile.printAttributeData(iday, daysOut, pw, macds, macdfunctionDaysDiff, arraypos, gsd.inClose,
//					bavg, savg);
			printAttributeData(iday, daysOut, pw, macds, macdfunctionDaysDiff, doubleBacks, arraypos, gsd.inClose, db,
					withAttributePosition);
			dateAttribute.put(gsd.inDate[iday], ++attributePos);
			iday++;
			for (pos = 0; pos < arraypos.length; pos++) {
				arraypos[pos]++;
			}

		}

		pw.close();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(ArrayList<double[]> macdData, int macdfunctionDaysDiff, int macdstart,
			int doubleBack) {
		return macdData.get(0)[macdstart - doubleBack] + ","
				+ macdData.get(1)[macdstart - (macdfunctionDaysDiff + doubleBack)] + ",";

	}

	public void printAttributeData(int iday, int daysOut, PrintWriter pw, TreeMap<String, Object> macds,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition) {
		int pos = 0;
		for (String key : macds.keySet()) {

			ArrayList<double[]> macdData = (ArrayList<double[]>) macds.get(key);
			int macdfunctionDaysDiff = functionDaysDiffMap.get(key);
			int macdstart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			// System.out.print(macdDates.get(key)[macdstart] + ";");
			pw.print(getAttributeText(macdData, macdfunctionDaysDiff, macdstart, doubleBack.intValue()));

			pos++;

		}

		if (withAttributePosition)
			pw.println(priceBands.getAttributePosition(iday, daysOut, closes));
		else
			pw.println(priceBands.getAttributeValue(iday, daysOut, closes));
	}

}
