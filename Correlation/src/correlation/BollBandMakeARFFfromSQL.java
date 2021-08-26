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
import com.tictactec.ta.lib.MAType;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import util.Realign;
import util.getDatabaseConnection;

public class BollBandMakeARFFfromSQL {

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
		BollBandMakeARFFfromSQL bb = new BollBandMakeARFFfromSQL();
		bb.makeARFFFromSQL(sym, dos);
	}

	public static String getFilename(String sym, String dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_bb_correlation.arff";
	}

	public TreeMap<String, Integer> dateAttribute = new TreeMap<>();

	public void makeARFFFromSQL(String sym, String dos) throws Exception {

		boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
		Core core = new Core();
		Connection conn = null;

		conn = getDatabaseConnection.makeConnection();

		PreparedStatement ps = conn
				.prepareStatement("select * from bb_correlation" + " where symbol=? and toCloseDays=?");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		String startDate = gsd.inDate[50];

		TreeMap<String, Object> bbs = new TreeMap<>();
		TreeMap<String, Integer> bbfunctionDaysDiff = new TreeMap<>();
		TreeMap<String, Integer> doubleBacks = new TreeMap<>();

		TreeMap<String, String[]> bbDates = new TreeMap<>();
		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		PrintWriter pw = new PrintWriter(getFilename(sym, dos));
		pw.println("% 1. Title: " + sym + "_bb_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL BollBandsGSD = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(BollBandsGSD.inDate[50]) < 0)
				startDate = BollBandsGSD.inDate[50];

			int period = rs.getInt("period");
			double stddev = rs.getDouble("stddev");

			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();

			double[] outRealUpperBand = new double[BollBandsGSD.inClose.length];
			double[] outRealMiddleBand = new double[BollBandsGSD.inClose.length];
			double[] outRealLowerBand = new double[BollBandsGSD.inClose.length];
			core.bbands(0, BollBandsGSD.inClose.length - 1, BollBandsGSD.inClose, period, stddev, stddev, MAType.Ema,
					outBegIdx, outNBElement, outRealUpperBand, outRealMiddleBand, outRealLowerBand);

			Realign.realign(outRealUpperBand, outBegIdx);
			Realign.realign(outRealMiddleBand, outBegIdx);
			Realign.realign(outRealLowerBand, outBegIdx);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");
			ArrayList<double[]> bbData = new ArrayList<>();
			bbData.add(outRealUpperBand);
			bbData.add(outRealMiddleBand);
			bbData.add(outRealLowerBand);

			bbs.put(symKey, bbData);
			bbfunctionDaysDiff.put(symKey, rs.getInt("functionDaysDiff"));
			doubleBacks.put(symKey, rs.getInt("doubleBack"));
			pw.println("@ATTRIBUTE " + symKey + "bbMiddle1 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "bbMiddle2 NUMERIC");
			// pw.println("@ATTRIBUTE " + symKey + "bbMiddle3 NUMERIC");
			bbDates.put(symKey, BollBandsGSD.inDate);
		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int arraypos[] = new int[bbs.size()];
		int pos = 50;
		 
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		int attributePos = -1;
		eemindexLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut - 1;) {
			String posDate = gsd.inDate[iday];
			pos = 0;
			for (String key : bbs.keySet()) {
				String sdate = bbDates.get(key)[arraypos[pos]];
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
			for (String key : bbs.keySet()) {
				int giDay = iday - bbfunctionDaysDiff.get(key);
				int siDay = arraypos[pos] - bbfunctionDaysDiff.get(key);

				for (int i = 0; i < (bbfunctionDaysDiff.get(key) + daysOut); i++) {
					String gtest = gsd.inDate[giDay + i];
					String stest = bbDates.get(key)[siDay + i];
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

//			MACDMakeARFFfromCorrelationFile.printAttributeData(iday, daysOut, pw, bbs, bbfunctionDaysDiff, arraypos, gsd.inClose,
//					bavg, savg);
			printAttributeData(iday, daysOut, pw, bbs, bbfunctionDaysDiff, doubleBacks, arraypos, gsd.inClose, db,
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

	public String getAttributeText(ArrayList<double[]> bbData, int bbfunctionDaysDiff, int bbstart, int doubleBack) {
		return (bbData.get(1)[bbstart - (doubleBack)] - bbData.get(1)[bbstart - (bbfunctionDaysDiff + doubleBack)])
				+ "," + (bbData.get(1)[bbstart - (doubleBack) - 1]
						- bbData.get(1)[bbstart - (bbfunctionDaysDiff + doubleBack)] - 1)
//				+ "," + (bbData.get(1)[bbstart - (doubleBack) - 2]
//						- bbData.get(1)[bbstart - (bbfunctionDaysDiff + doubleBack)] - 2)
				+ ",";

	}

	public void printAttributeData(int iday, int daysOut, PrintWriter pw, TreeMap<String, Object> bbs,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition) {
		int pos = 0;
		for (String key : bbs.keySet()) {

			ArrayList<double[]> bbData = (ArrayList<double[]>) bbs.get(key);
			int bbfunctionDaysDiff = functionDaysDiffMap.get(key);
			int bbstart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;

			pw.print(getAttributeText(bbData, bbfunctionDaysDiff, bbstart, doubleBack.intValue()));

			pos++;

		}
		if (withAttributePosition)
			pw.println(priceBands.getAttributePosition(iday, daysOut, closes));
		else
			pw.println(priceBands.getAttributeValue(iday, daysOut, closes));
	}

}
