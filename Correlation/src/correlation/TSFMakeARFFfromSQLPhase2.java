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

public class TSFMakeARFFfromSQLPhase2 {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
	boolean makeWith30Days = true;

	public TSFMakeARFFfromSQLPhase2(boolean b, boolean makeWith30) {
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
		TSFMakeARFFfromSQLPhase2 tsf = new TSFMakeARFFfromSQLPhase2(false, true);
		Connection conn = getDatabaseConnection.makeConnection();
		String data = tsf.makeARFFFromSQL(sym, dos, conn, false);
		File file = new File(getFilename(sym, dos));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();

	}

	public static String getFilename(String sym, String dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_tsf2_correlation.arff";
	}

	public String makeARFFFromSQL(String sym, String dos, Connection conn, boolean includeLastQuestionMark)
			throws Exception {

		Core core = new Core();

		TSFParms tsfp = new TSFParms();
		PreparedStatement ps = conn.prepareStatement("select * from tsf_correlation" + (makeWith30Days ? "" : "_180")
				+ " where symbol=? and toCloseDays=?  ");

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
		pw.println("% 1. Title: " + sym + "_tsf_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");
			GetETFDataUsingSQL gsdTSF = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(gsdTSF.inDate[pos]) < 0)
				startDate = gsdTSF.inDate[pos];

			int tsfPeriod = rs.getInt("tsfPeriod");
			double tsf[] = new double[gsdTSF.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.tsf(0, gsdTSF.inClose.length - 1, gsdTSF.inClose, tsfPeriod, outBegIdx, outNBElement, tsf);

			Realign.realign(tsf, outBegIdx.value);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");

			tsfp.addSymbol(symKey);
			tsfp.settsfs(symKey, tsf);
			tsfp.setDoubleBacks(symKey, rs.getInt("doubleBack"));
			tsfp.setDaysDiff(symKey, rs.getInt("functionDaysDiff"));
			tsfp.setAttrDates(symKey, gsdTSF.inDate);

			pw.println("@ATTRIBUTE " + symKey + "tsf NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "tsfBack NUMERIC");
		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		for (int iday = pos; iday < gsd.inDate.length - daysOut - 1; iday++) {
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, tsfp, gsd.inClose, db,
					withAttributePosition, false);
			if (sb != null)
				pw.print(sb.toString());
		}

		if (includeLastQuestionMark) {
			int iday = gsd.inDate.length - 1;
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, tsfp, gsd.inClose, db,
					withAttributePosition, true);
			if (sb != null)
				pw.print(sb.toString());
		}
		return sw.toString();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(double tsf[], int tsffunctionDaysDiff, int tsfstart, int doubleBack) {

		return tsf[tsfstart - doubleBack] + "," + tsf[tsfstart - (tsffunctionDaysDiff + doubleBack)] + ",";

	}

	public StringBuffer printAttributeData(int iday, String etfDates[], int daysOut, AttributeParm tsfp,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition, boolean forLastUseQuestionMark) {
		StringBuffer returnBuffer = new StringBuffer(1000);

		for (String key : tsfp.keySet()) {

			String dates[] = tsfp.getAttrDates(key);
			int tsfstart = tsfp.getLastDateStart(key);
			if (tsfstart < 75)
				tsfstart = 75;
			int tsffunctionDaysDiff = tsfp.getDaysDiff(key);
			for (; tsfstart < dates.length; tsfstart++) {
				if (dates[tsfstart].compareTo(etfDates[iday]) == 0)
					break;
				if (dates[tsfstart].compareTo(etfDates[iday]) > 0)
					return null;
			}

			// are we missing any days in between
			int giDay = iday - tsffunctionDaysDiff;
			int siDay = tsfstart - tsffunctionDaysDiff;
			// are we missing any days in between
			if (!forLastUseQuestionMark)
				for (int i = 0; i < (tsffunctionDaysDiff + daysOut); i++) {
					String gtest = etfDates[giDay + i];
					String stest = dates[siDay + i];
					if (gtest.compareTo(stest) != 0) {
						return null;
					}
				}
			tsfp.setLastDateStart(key, tsfstart);
			double tsfs[] = ((TSFParms) tsfp).gettsfs(key);
			Integer doubleBack = tsfp.getDoubleBacks(key);
			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(tsfs, tsffunctionDaysDiff, tsfstart, doubleBack.intValue()));
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
