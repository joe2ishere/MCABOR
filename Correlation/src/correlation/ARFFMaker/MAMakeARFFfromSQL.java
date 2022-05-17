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
import com.tictactec.ta.lib.MAType;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import correlation.ARFFMaker.Parms.AttributeParm;
import correlation.ARFFMaker.Parms.MAParms;
import util.Realign;
import util.getDatabaseConnection;

public class MAMakeARFFfromSQL extends AttributeMakerFromSQL {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
	boolean makeWith30Days = true;

	public MAMakeARFFfromSQL(boolean b, boolean makeWith30) {
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
		MAMakeARFFfromSQL ma = new MAMakeARFFfromSQL(false, true);
		Connection conn = getDatabaseConnection.makeConnection();
		int daysOut = Integer.parseInt(dos);
		AttributeParm parms = ma.buildParameters(sym, daysOut, conn);
		String data = ma.makeARFFFromSQL(sym, daysOut, parms, true);
		File file = new File(getFilename(sym, daysOut));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, int dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_ma2bb_correlation.arff";
	}

	@Override
	public AttributeParm buildParameters(String sym, int daysOut, Connection conn) throws Exception {
		Core core = new Core();
		MAParms parms = new MAParms();

		PreparedStatement ps = conn.prepareStatement("select * from ma_correlation" + (makeWith30Days ? "" : "_130")
				+ " where symbol=? and toCloseDays=?  ");
		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			String functionSymbol = rs.getString("functionSymbol");
			GetETFDataUsingSQL gsdMA = GetETFDataUsingSQL.getInstance(functionSymbol);
			int maPeriod = rs.getInt("period");
			String type = rs.getString("matype");
			MAType maType;
			if (type.startsWith("Sma"))
				maType = MAType.Sma;
			else if (type.startsWith("Ema"))
				maType = MAType.Ema;
			else {
				System.out.println("error getting type " + type);
				System.exit(0);
				return null;
			}
			double ma[] = new double[gsdMA.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.movingAverage(0, gsdMA.inClose.length - 1, gsdMA.inHigh, maPeriod, maType, outBegIdx, outNBElement,
					ma);
			Realign.realign(ma, outBegIdx.value);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");
			parms.addSymbol(symKey, rs.getInt("functionDaysDiff"), rs.getInt("doubleBack"), gsdMA.dateIndex, ma);

		}
		return parms;
	}

	@Override
	public void printAttributeHeader(PrintWriter pw, String sym, int daysOut, DeltaBands db, AttributeParm parms,
			boolean allHeaderFields) {

		if (allHeaderFields) {
			pw.println("% 1. Title: " + sym + "_ma_correlation");
			pw.println("@RELATION " + sym + "_" + daysOut);
		}
		for (String symKey : parms.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "ma NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");

		}

		if (allHeaderFields) {
			if (withAttributePosition)
				pw.println("@ATTRIBUTE class NUMERIC");
			else
				pw.println(db.getAttributeDefinition());
		}

	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(double ma[], int mafunctionDaysDiff, int mastart, int doubleBack) {
		return ma[mastart - doubleBack] + "," + ma[mastart - (mafunctionDaysDiff + doubleBack)] + ",";

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
			double mas[] = ((MAParms) parms).getMAs(key);
			Integer doubleBack = parms.getDoubleBacks(key);
			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(mas, functionDaysDiff, dateidx, doubleBack.intValue()));
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
