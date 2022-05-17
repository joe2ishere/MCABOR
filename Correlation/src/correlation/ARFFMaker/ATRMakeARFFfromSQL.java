package correlation.ARFFMaker;

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
import correlation.ARFFMaker.Parms.ATRParms;
import correlation.ARFFMaker.Parms.AttributeParm;
import util.Realign;
import util.getDatabaseConnection;

public class ATRMakeARFFfromSQL extends AttributeMakerFromSQL {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
	boolean makeWith30Days = true;

	public ATRMakeARFFfromSQL(boolean b, boolean makeWith30) {
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
		ATRMakeARFFfromSQL atr = new ATRMakeARFFfromSQL(false, true);
		Connection conn = getDatabaseConnection.makeConnection();
		int daysOut = Integer.parseInt(dos);
		AttributeParm parms = atr.buildParameters(sym, daysOut, conn);
		String data = atr.makeARFFFromSQL(sym, daysOut, parms, false);
		File file = new File(getFilename(sym, daysOut));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, int dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_atr2bb_correlation.arff";
	}

	@Override
	public AttributeParm buildParameters(String sym, int daysOut, Connection conn) throws Exception {
		Core core = new Core();
		ATRParms parms = new ATRParms();

		PreparedStatement ps = conn.prepareStatement("select * from atr_correlation" + (makeWith30Days ? "" : "_130")
				+ " where symbol=? and toCloseDays=?  ");
		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			String functionSymbol = rs.getString("functionSymbol");
			GetETFDataUsingSQL gsdATR = GetETFDataUsingSQL.getInstance(functionSymbol);
			int atrPeriod = rs.getInt("atrPeriod");
			double atr[] = new double[gsdATR.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.atr(0, gsdATR.inClose.length - 1, gsdATR.inHigh, gsdATR.inLow, gsdATR.inClose, atrPeriod, outBegIdx,
					outNBElement, atr);
			Realign.realign(atr, outBegIdx.value);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");
			parms.addSymbol(symKey, rs.getInt("functionDaysDiff"), rs.getInt("doubleBack"), gsdATR.dateIndex, atr);
		}
		return parms;
	}

	@Override

	public void printAttributeHeader(PrintWriter pw, String sym, int daysOut, DeltaBands db, AttributeParm parms,
			boolean allHeaderFields) {

		if (allHeaderFields) {
			pw.println("% 1. Title: " + sym + "_atr_correlation");
			pw.println("@RELATION " + sym + "_" + daysOut);
		}

		for (String symKey : parms.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "atr NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "atrBack NUMERIC");
		}
		if (allHeaderFields) {
			if (withAttributePosition)
				pw.println("@ATTRIBUTE class NUMERIC");
			else
				pw.println(db.getAttributeDefinition());
		}
	}

	@Override
	public String makeARFFFromSQLForQuestionMark(String sym, int daysOut, AttributeParm parms) throws Exception {

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_atr_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);
		for (String symKey : parms.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "atr NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "atrBack NUMERIC");

		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int iday = gsd.inDate.length - 1;
		StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, parms, gsd.inClose, db, withAttributePosition,
				true);
		if (sb != null) {
			pw.print(sb.toString());

		}

		return sw.toString();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(double atr[], int atrfunctionDaysDiff, int atrstart, int doubleBack) {
		return atr[atrstart - doubleBack] + "," + atr[atrstart - (atrfunctionDaysDiff + doubleBack)] + ",";

	}

	public StringBuffer printAttributeData(int iday, String etfDates[], int daysOut, AttributeParm parms,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition, boolean forLastUseQuestionMark) {
		StringBuffer returnBuffer = new StringBuffer(100);

		for (String key : parms.keySet()) {

			int functionDaysDiff = parms.getDaysDiff(key);
			Integer dateidx = parms.getDateIndex(key).get(etfDates[iday]);
			if (dateidx == null)
				return null;
			// are we missing any days in between
			int giDay = iday - functionDaysDiff;
			// are we missing any days in between
			if (!forLastUseQuestionMark)
				for (int i = 0; i < (functionDaysDiff + daysOut); i++) {
					String gtest = etfDates[giDay + i];
					if (parms.getDateIndex(key).containsKey(gtest) == false) {
						return null;
					}
				}

			double atrs[] = ((ATRParms) parms).getATRs(key);
			Integer doubleBack = parms.getDoubleBacks(key);
			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(atrs, functionDaysDiff, dateidx, doubleBack.intValue()));
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
