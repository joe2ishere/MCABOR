package correlation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
		BBParms bbp = new BBParms();
		Core core = new Core();
		Connection conn = null;

		conn = getDatabaseConnection.makeConnection();

		PreparedStatement ps = conn
				.prepareStatement("select * from bb_correlation" + " where symbol=? and toCloseDays=?");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		String startDate = gsd.inDate[50];

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

			bbp.addSymbol(symKey);
			bbp.setBBs(symKey, bbData);
			bbp.setDoubleBacks(symKey, rs.getInt("doubleBack"));
			bbp.setDaysDiff(symKey, rs.getInt("functionDaysDiff"));
			bbp.setAttrDates(symKey, BollBandsGSD.inDate);

			pw.println("@ATTRIBUTE " + symKey + "bbMiddle1 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "bbMiddle2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "bbMiddle3 NUMERIC");

		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int pos = 50;

		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		for (int iday = pos; iday < gsd.inDate.length - daysOut - 1; iday++) {

			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, bbp, gsd.inClose, db, withAttributePosition,
					false);
			if (sb != null)
				pw.print(sb.toString());

		}

		pw.close();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(ArrayList<double[]> bbData, int bbfunctionDaysDiff, int bbstart, int doubleBack) {
		return (bbData.get(1)[bbstart - (doubleBack)] - bbData.get(1)[bbstart - (bbfunctionDaysDiff + doubleBack)])
				+ ","
				+ (bbData.get(1)[bbstart - (doubleBack) - 1]
						- bbData.get(1)[bbstart - (bbfunctionDaysDiff + doubleBack)] - 1)
				+ "," + (bbData.get(1)[bbstart - (doubleBack) - 2]
						- bbData.get(1)[bbstart - (bbfunctionDaysDiff + doubleBack)] - 2)
				+ ",";

	}

	public StringBuffer printAttributeData(int iday, String etfDates[], int daysOut, BBParms bbp, double[] closes,
			DeltaBands priceBands, boolean withAttributePosition, boolean forLastUseQuestionMark) {
		StringBuffer returnBuffer = new StringBuffer(1000);

		for (String key : bbp.keySet()) {
			String dates[] = bbp.getAttrDates(key);
			int bbstart = bbp.getLastDateStart(key);
			for (; bbstart < dates.length; bbstart++) {
				if (dates[bbstart].compareTo(etfDates[iday]) == 0)
					break;
				if (dates[bbstart].compareTo(etfDates[iday]) > 0)
					return null;
			}
			int bbfunctionDaysDiff = bbp.getDaysDiff(key);
			Integer doubleBack = bbp.getDoubleBacks(key);
			// are we missing any days in between
			int giDay = iday - bbfunctionDaysDiff;
			int siDay = bbstart - bbfunctionDaysDiff;
			// are we missing any days in between
			for (int i = 0; i < (bbfunctionDaysDiff + daysOut); i++) {
				String gtest = etfDates[giDay + i];
				String stest = dates[siDay + i];
				if (gtest.compareTo(stest) != 0) {
					return null;
				}
			}
			bbp.setLastDateStart(key, bbstart);
			ArrayList<double[]> bbData = bbp.getBBs(key);

			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(bbData, bbfunctionDaysDiff, bbstart, doubleBack.intValue()));

		}
		if (forLastUseQuestionMark)
			returnBuffer.append("?");
		else if (withAttributePosition)
			returnBuffer.append(priceBands.getAttributePosition(iday, daysOut, closes));
		else
			returnBuffer.append(priceBands.getAttributeValue(iday, daysOut, closes));

		returnBuffer.append("\n");
		return returnBuffer;
	}

}
