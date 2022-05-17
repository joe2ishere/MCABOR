package correlation.ARFFMaker;

import java.io.File;
import java.io.PrintWriter;
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
import correlation.ARFFMaker.Parms.AttributeParm;
import correlation.ARFFMaker.Parms.MACDParms;
import util.Realign;
import util.getDatabaseConnection;

public class MACDMakeARFFfromSQL extends AttributeMakerFromSQL {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
	boolean makeWith30Days = true;

	public MACDMakeARFFfromSQL(boolean b, boolean makeWith30) {
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
		MACDMakeARFFfromSQL macd = new MACDMakeARFFfromSQL(false, true);
		Connection conn = getDatabaseConnection.makeConnection();
		int daysOut = Integer.parseInt(dos);
		AttributeParm parms = macd.buildParameters(sym, daysOut, conn);
		String data = macd.makeARFFFromSQL(sym, daysOut, parms, false);
		File file = new File(getFilename(sym, daysOut));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, int dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_macd2bb_correlation.arff";
	}

	@Override
	public AttributeParm buildParameters(String sym, int daysOut, Connection conn) throws Exception {
		Core core = new Core();
		MACDParms parms = new MACDParms();
		PreparedStatement ps = conn.prepareStatement("select * from macd_correlation" + (makeWith30Days ? "" : "_130")
				+ " where symbol=? and toCloseDays=?");

		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(functionSymbol);

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

			parms.addSymbol(symKey, rs.getInt("functionDaysDiff"), rs.getInt("doubleBack"), pgsd.dateIndex, outMACD,
					outMACDSignal);

		}

		return parms;
	}

	public void printAttributeHeader(PrintWriter pw, String sym, int daysOut, DeltaBands db, AttributeParm parms) {
		pw.println("% 1. Title: " + sym + "_macd_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);

		for (String symKey : parms.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "macd NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "macd2 NUMERIC");
		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(double[] macd, double[] signal, int macdfunctionDaysDiff, int macdstart,
			int doubleBack) {
		return macd[macdstart] + "," /* + signal[macdstart - doubleBack] + "," */
				+ macd[macdstart - doubleBack] + "," /* + signal[macdstart - doubleBack] + "," */;

	}

	public StringBuffer printAttributeData(int iday, String etfDates[], int daysOut, AttributeParm parms,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition, boolean forLastUseQuestionMark) {
		StringBuffer returnBuffer = new StringBuffer(1000);
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

			double[] macd = ((MACDParms) parms).getMACD(key);
			double[] signal = ((MACDParms) parms).getSignal(key);

			Integer doubleBack = parms.getDoubleBacks(key);
			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(macd, signal, functionDaysDiff, dateidx, doubleBack.intValue()));
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
