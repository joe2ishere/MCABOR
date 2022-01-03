package correlation;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import util.Realign;
import util.getDatabaseConnection;

public class MACDMakeARFFfromSQLPhase2 {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
	boolean makeWith30Days = true;

	public MACDMakeARFFfromSQLPhase2(boolean b, boolean makeWith30) {
		withAttributePosition = b;
		makeWith30Days = makeWith30;
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
		MACDMakeARFFfromSQLPhase2 macd = new MACDMakeARFFfromSQLPhase2(false, true);
		Connection conn = getDatabaseConnection.makeConnection();
		String data = macd.makeARFFFromSQL(sym, dos, conn, false);
		File file = new File(getFilename(sym, dos));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, String dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_macd2_correlation.arff";
	}

	public String makeARFFFromSQL(String sym, String dos, Connection conn, boolean includeLastQuestionMark)
			throws Exception {

		Core core = new Core();

		MACDParms macdp = new MACDParms();
		PreparedStatement ps = conn.prepareStatement("select * from macd_correlation" + (makeWith30Days ? "" : "_180")
				+ " where symbol=? and toCloseDays=?");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		int pos = 25 + daysOut;
		String startDate = gsd.inDate[pos];

		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_macd_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(pgsd.inDate[pos]) < 0)
				startDate = pgsd.inDate[pos];

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

			macdp.addSymbol(symKey);
			macdp.setMACD(symKey, outMACD);
			macdp.setSignal(symKey, outMACDSignal);
			macdp.setDoubleBacks(symKey, rs.getInt("doubleBack"));
			macdp.setDaysDiff(symKey, rs.getInt("functionDaysDiff"));
			macdp.setAttrDates(symKey, pgsd.inDate);

			pw.println("@ATTRIBUTE " + symKey + "macd NUMERIC");
			// pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "macd2 NUMERIC");
			// pw.println("@ATTRIBUTE " + symKey + "signal2 NUMERIC");

		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		for (int iday = pos; iday < gsd.inDate.length - daysOut - 1; iday++) {
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, macdp, gsd.inClose, db,
					withAttributePosition, false);
			if (sb != null)
				pw.print(sb.toString());
		}

		if (includeLastQuestionMark) {
			int iday = gsd.inDate.length - 1;
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, macdp, gsd.inClose, db,
					withAttributePosition, true);
			if (sb != null)
				pw.print(sb.toString());
		}
		return sw.toString();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(double[] macd, double[] signal, int macdfunctionDaysDiff, int macdstart,
			int doubleBack) {
		return macd[macdstart] + "," /* + signal[macdstart - doubleBack] + "," */
				+ macd[macdstart - doubleBack] + "," /* + signal[macdstart - doubleBack] + "," */;

	}

	public StringBuffer printAttributeData(int iday, String etfDates[], int daysOut, AttributeParm macdp,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition, boolean forLastUseQuestionMark) {
		StringBuffer returnBuffer = new StringBuffer(1000);
		for (String key : macdp.keySet()) {
			String dates[] = macdp.getAttrDates(key);
			int macdstart = macdp.getLastDateStart(key);
			int macdfunctionDaysDiff = macdp.getDaysDiff(key);
			for (; macdstart < dates.length; macdstart++) {
				if (dates[macdstart].compareTo(etfDates[iday]) == 0)
					break;
				if (dates[macdstart].compareTo(etfDates[iday]) > 0)
					return null;
			}

			// are we missing any days in between
			int giDay = iday - macdfunctionDaysDiff;
			int siDay = macdstart - macdfunctionDaysDiff;
			// are we missing any days in between
			if (!forLastUseQuestionMark)
				for (int i = 0; i < (macdfunctionDaysDiff + daysOut); i++) {
					String gtest = etfDates[giDay + i];
					String stest = dates[siDay + i];
					if (gtest.compareTo(stest) != 0) {
						return null;
					}
				}
			macdp.setLastDateStart(key, macdstart);
			double[] macd = ((MACDParms) macdp).getMACD(key);
			double[] signal = ((MACDParms) macdp).getSignal(key);

			Integer doubleBack = macdp.getDoubleBacks(key);
			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(macd, signal, macdfunctionDaysDiff, macdstart, doubleBack.intValue()));
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
