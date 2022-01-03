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

public class DMIMakeARFFfromSQLPhase2 {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
	boolean makeWith30Days = true;

	public DMIMakeARFFfromSQLPhase2(boolean b, boolean makeWith30) {
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
		DMIMakeARFFfromSQLPhase2 dmi = new DMIMakeARFFfromSQLPhase2(false, true);
		Connection conn = getDatabaseConnection.makeConnection();
		String data = dmi.makeARFFFromSQL(sym, dos, conn, false);
		File file = new File(getFilename(sym, dos));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, String dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_dmi2_correlation.arff";
	}

	public String makeARFFFromSQL(String sym, String dos, Connection conn, boolean includeLastQuestionMark)
			throws Exception {

		Core core = new Core();

		DMIParms dmip = new DMIParms();

		PreparedStatement ps = conn.prepareStatement("select * from dmi_correlation" + (makeWith30Days ? "" : "_180")
				+ " where symbol=? and toCloseDays=?  ");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		int pos = 25 + daysOut;
		String startDate = gsd.inDate[pos];

		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		String in;
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_dmi_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL gsdDMI = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(gsdDMI.inDate[pos]) < 0)
				startDate = gsdDMI.inDate[pos];

			int dmiPeriod = rs.getInt("dmiPeriod");

			double dmi[] = new double[gsdDMI.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.dx(0, gsdDMI.inClose.length - 1, gsdDMI.inHigh, gsdDMI.inLow, gsdDMI.inClose, dmiPeriod, outBegIdx,
					outNBElement, dmi);

			Realign.realign(dmi, outBegIdx.value);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");
			dmip.addSymbol(symKey);
			dmip.setDMIs(symKey, dmi);
			dmip.setDoubleBacks(symKey, rs.getInt("doubleBack"));
			dmip.setDaysDiff(symKey, rs.getInt("functionDaysDiff"));
			dmip.setAttrDates(symKey, gsdDMI.inDate);
			pw.println("@ATTRIBUTE " + symKey + "dmi NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");

		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		for (int iday = pos; iday < gsd.inDate.length - daysOut - 1; iday++) {
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, dmip, gsd.inClose, db,
					withAttributePosition, false);
			if (sb != null)
				pw.print(sb.toString());
		}

		if (includeLastQuestionMark) {
			int iday = gsd.inDate.length - 1;
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, dmip, gsd.inClose, db,
					withAttributePosition, true);
			if (sb != null)
				pw.print(sb.toString());
		}

		return sw.toString();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(double dmi[], int dmifunctionDaysDiff, int dmistart, int doubleBack) {
		return dmi[dmistart - doubleBack] + "," + dmi[dmistart - (dmifunctionDaysDiff + doubleBack)] + ",";

	}

	public StringBuffer printAttributeData(int iday, String etfDates[], int daysOut, AttributeParm dmip,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition, boolean forLastUseQuestionMark) {
		StringBuffer returnBuffer = new StringBuffer(100);

		for (String key : dmip.keySet()) {

			String dates[] = dmip.getAttrDates(key);
			int dmistart = dmip.getLastDateStart(key);
			int dmifunctionDaysDiff = dmip.getDaysDiff(key);
			for (; dmistart < dates.length; dmistart++) {
				if (dates[dmistart].compareTo(etfDates[iday]) == 0)
					break;
				if (dates[dmistart].compareTo(etfDates[iday]) > 0)
					return null;
			}

			// are we missing any days in between
			int giDay = iday - dmifunctionDaysDiff;
			int siDay = dmistart - dmifunctionDaysDiff;
			// are we missing any days in between
			if (!forLastUseQuestionMark)
				for (int i = 0; i < (dmifunctionDaysDiff + daysOut); i++) {
					String gtest = etfDates[giDay + i];
					String stest = dates[siDay + i];
					if (gtest.compareTo(stest) != 0) {
						return null;
					}
				}
			dmip.setLastDateStart(key, dmistart);
			double dmis[] = ((DMIParms) dmip).getDMIs(key);
			Integer doubleBack = dmip.getDoubleBacks(key);
			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(dmis, dmifunctionDaysDiff, dmistart, doubleBack.intValue()));
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
