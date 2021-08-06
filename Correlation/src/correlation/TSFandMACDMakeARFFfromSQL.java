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

public class TSFandMACDMakeARFFfromSQL {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5

	public TSFandMACDMakeARFFfromSQL(boolean b) {
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
		TSFandMACDMakeARFFfromSQL tsf = new TSFandMACDMakeARFFfromSQL(false);
		tsf.makeARFFFromSQL(sym, dos);
	}

	public String getFilename(String sym, String dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_tsfandmacd_correlation.arff";
	}

	public TreeMap<String, Integer> dateAttribute = new TreeMap<>();

	public void makeARFFFromSQL(String sym, String dos) throws Exception {

		Core core = new Core();
		Connection conn = null;

		conn = getDatabaseConnection.makeConnection();

		PreparedStatement psTSF = conn
				.prepareStatement("select * from tsf_correlation" + " where symbol=? and toCloseDays=?  ");

		PreparedStatement psMACD = conn
				.prepareStatement("select * from macd_correlation" + " where symbol=? and toCloseDays=?");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		String startDate = gsd.inDate[50];

		TreeMap<String, double[]> tsfs = new TreeMap<>();
		TreeMap<String, Integer> tsffunctionDaysDiff = new TreeMap<>();
		TreeMap<String, Integer> tsfDoubleBacks = new TreeMap<>();
		TreeMap<String, String[]> tsfDates = new TreeMap<>();

		TreeMap<String, Object> macds = new TreeMap<>();
		TreeMap<String, Integer> macdfunctionDaysDiff = new TreeMap<>();
		TreeMap<String, Integer> macdDoubleBacks = new TreeMap<>();
		TreeMap<String, String[]> macdDates = new TreeMap<>();

		psTSF.setString(1, sym);
		psTSF.setInt(2, daysOut);
		ResultSet rsTSF = psTSF.executeQuery();
		psMACD.setString(1, sym);
		psMACD.setInt(2, daysOut);
		ResultSet rsMACD = psMACD.executeQuery();

		PrintWriter pw = new PrintWriter(getFilename(sym, dos));
		pw.println("% 1. Title: " + sym + "_tsfandmacd_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rsTSF.next() & rsMACD.next()) {

			String tsfFunctionSymbol = rsTSF.getString("functionSymbol");
			GetETFDataUsingSQL gsdTSF = GetETFDataUsingSQL.getInstance(tsfFunctionSymbol);

			if (startDate.compareTo(gsdTSF.inDate[50]) < 0)
				startDate = gsdTSF.inDate[50];

			int tsfPeriod = rsTSF.getInt("tsfPeriod");

			double tsf[] = new double[gsdTSF.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.tsf(0, gsdTSF.inClose.length - 1, gsdTSF.inClose, tsfPeriod, outBegIdx, outNBElement, tsf);

			Realign.realign(tsf, outBegIdx.value);
			String symKey = tsfFunctionSymbol + "_" + rsTSF.getInt("significantPlace");
			tsfs.put(symKey, tsf);
			int doubleBack = rsTSF.getInt("doubleBack");
			tsfDoubleBacks.put(symKey, doubleBack);
			tsffunctionDaysDiff.put(symKey, rsTSF.getInt("functionDaysDiff"));
			pw.println("@ATTRIBUTE " + symKey + "tsf NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "tsfBack NUMERIC");
			tsfDates.put(symKey, gsdTSF.inDate);

			String functionSymbol = rsMACD.getString("functionSymbol");

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(pgsd.inDate[50]) < 0)
				startDate = pgsd.inDate[50];

			int fastPeriod = rsMACD.getInt("fastPeriod");
			int slowPeriod = rsMACD.getInt("slowPeriod");
			int signalPeriod = rsMACD.getInt("signalPeriod");
			outBegIdx = new MInteger();
			outNBElement = new MInteger();
			double outMACD[] = new double[pgsd.inClose.length];
			double[] outMACDSignal = new double[pgsd.inClose.length];
			double[] outMACDHist = new double[pgsd.inClose.length];
			core.macd(0, pgsd.inClose.length - 1, pgsd.inClose, fastPeriod, slowPeriod, signalPeriod, outBegIdx,
					outNBElement, outMACD, outMACDSignal, outMACDHist);

			Realign.realign(outMACD, outBegIdx);
			Realign.realign(outMACDSignal, outBegIdx);
			Realign.realign(outMACDHist, outBegIdx);
			symKey = functionSymbol + "_" + rsMACD.getInt("significantPlace");
			ArrayList<double[]> macdData = new ArrayList<>();
			macdData.add(outMACD);
			macdData.add(outMACDSignal);

			macds.put(symKey, macdData);
			macdfunctionDaysDiff.put(symKey, rsMACD.getInt("functionDaysDiff"));
			macdDoubleBacks.put(symKey, rsMACD.getInt("doubleBack"));
			pw.println("@ATTRIBUTE " + symKey + "macd NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			macdDates.put(symKey, pgsd.inDate);

		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		// need to align tsf and macd table dates with the gsd dates
		// one problem is that there are missing dates in some of the tables

		int tsfArrayPos[] = new int[tsfs.size()];
		int pos = 0;
		for (String key : tsfs.keySet()) {
			tsfArrayPos[pos] = 0;
		}
		int macdArrayPos[] = new int[macds.size()];
		for (String key : macds.keySet()) {
			macdArrayPos[pos] = 0;
		}
		pos = 50;
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		int attributePos = -1;
		dateLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut - 1;) {
			String posDate = gsd.inDate[iday];
			// * TSF START **/
			pos = 0;
			for (String key : tsfs.keySet()) {
				String sdate = tsfDates.get(key)[tsfArrayPos[pos]];
				int dcomp = posDate.compareTo(sdate);
				if (dcomp < 0) {
					iday++;
					continue dateLoop;
				}
				if (dcomp > 0) {
					tsfArrayPos[pos]++;
					continue dateLoop;
				}
				pos++;
			}

			pos = 0;
			for (String key : tsfs.keySet()) {
				int giDay = iday - tsffunctionDaysDiff.get(key);
				int siDay = tsfArrayPos[pos] - tsffunctionDaysDiff.get(key);

				for (int i = 0; i < (tsffunctionDaysDiff.get(key) + daysOut); i++) {
					String gtest = gsd.inDate[giDay + i];
					String stest = tsfDates.get(key)[siDay + i];
					if (gtest.compareTo(stest) != 0) {
						iday++;
						continue dateLoop;
					}
				}
				pos++;
			}
			// * TSF END **/

			// * MACD START **/
			pos = 0;
			for (String key : macds.keySet()) {
				String sdate = macdDates.get(key)[macdArrayPos[pos]];
				int dcomp = posDate.compareTo(sdate);
				if (dcomp < 0) {
					iday++;
					continue dateLoop;
				}
				if (dcomp > 0) {
					macdArrayPos[pos]++;
					continue dateLoop;
				}
				pos++;
			}

			pos = 0;
			for (String key : macds.keySet()) {
				int giDay = iday - macdfunctionDaysDiff.get(key);
				int siDay = macdArrayPos[pos] - macdfunctionDaysDiff.get(key);

				for (int i = 0; i < (macdfunctionDaysDiff.get(key) + daysOut); i++) {
					String gtest = gsd.inDate[giDay + i];
					String stest = macdDates.get(key)[siDay + i];
					if (gtest.compareTo(stest) != 0) {
						iday++;
						continue dateLoop;
					}
				}
				pos++;
			}
			// * MACD END **/

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

			printAttributeData(iday, daysOut, pw, tsfs, tsffunctionDaysDiff, tsfDoubleBacks, tsfArrayPos, macds,
					macdfunctionDaysDiff, macdDoubleBacks, macdArrayPos, gsd.inClose, db, withAttributePosition);
			dateAttribute.put(gsd.inDate[iday], ++attributePos);
			iday++;
			for (pos = 0; pos < tsfArrayPos.length; pos++) {
				tsfArrayPos[pos]++;
			}

		}

		pw.close();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getTSFAttributeText(double tsf[], int tsffunctionDaysDiff, int tsfstart, int doubleBack) {
		return tsf[tsfstart - doubleBack] + "," + tsf[tsfstart - (tsffunctionDaysDiff + doubleBack)] + ",";

	}

	public String getMACDAttributeText(ArrayList<double[]> macdData, int macdfunctionDaysDiff, int macdstart,
			int doubleBack) {
		return macdData.get(0)[macdstart - doubleBack] + ","
				+ macdData.get(1)[macdstart - (macdfunctionDaysDiff + doubleBack)] + ",";

	}

	public void printAttributeData(int iday, int daysOut, PrintWriter pw, Object tsfsParms,
			TreeMap<String, Integer> tsfFunctionDaysDiffMap, TreeMap<String, Integer> tsfDoubleBacks, int[] tsfArrayPos,
			Object macdsParms, TreeMap<String, Integer> macdfunctionDaysDiffMap,
			TreeMap<String, Integer> macdDoubleBacks, int[] macdArrayPos, double[] closes, DeltaBands priceBands,
			boolean withAttributePosition) {

		TreeMap<String, double[]> tsfs = (TreeMap<String, double[]>) tsfsParms;
		int pos = 0;
		for (String key : tsfs.keySet()) {

			double tsf[] = tsfs.get(key);
			int tsffunctionDaysDiff = tsfFunctionDaysDiffMap.get(key);
			int tsfstart = tsfArrayPos[pos];
			Integer doubleBack = tsfDoubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getTSFAttributeText(tsf, tsffunctionDaysDiff, tsfstart, doubleBack.intValue()));

			pos++;

		}
		TreeMap<String, Object> macds = (TreeMap<String, Object>) macdsParms;
		pos = 0;
		for (String key : macds.keySet()) {

			ArrayList<double[]> macd = (ArrayList<double[]>) macds.get(key);
			int macdfunctionDaysDiff = macdfunctionDaysDiffMap.get(key);
			int macdstart = macdArrayPos[pos];
			Integer doubleBack = macdDoubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getMACDAttributeText(macd, macdfunctionDaysDiff, macdstart, doubleBack.intValue()));

			pos++;

		}

		if (withAttributePosition)
			pw.println(priceBands.getAttributePosition(iday, daysOut, closes));
		else
			pw.println(priceBands.getAttributeValue(iday, daysOut, closes));
	}

}
