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

public class TSFMakeARFFfromSQL extends AttributeMakerFromSQL {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
	boolean makeWith30Days = true;

	public TSFMakeARFFfromSQL(boolean b, boolean makeWith30) {
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
		TSFMakeARFFfromSQL tsf = new TSFMakeARFFfromSQL(false, true);
		Connection conn = getDatabaseConnection.makeConnection();
		int daysOut = Integer.parseInt(dos);
		AttributeParm parms = tsf.buildParameters(sym, daysOut, conn);
		String data = tsf.makeARFFFromSQL(sym, daysOut, parms, false);
		File file = new File(getFilename(sym, daysOut));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();

	}

	public static String getFilename(String sym, int dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_tsf2_correlation.arff";
	}

	@Override
	public AttributeParm buildParameters(String sym, int daysOut, Connection conn) throws Exception {
		Core core = new Core();
		TSFParms parms = new TSFParms();
		PreparedStatement ps = conn.prepareStatement("select * from tsf_correlation" + (makeWith30Days ? "" : "_130")
				+ " where symbol=? and toCloseDays=?   ");

		ps.setString(1, sym);
		ps.setInt(2, daysOut);

		ResultSet rs = ps.executeQuery();

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");
			GetETFDataUsingSQL gsdTSF = GetETFDataUsingSQL.getInstance(functionSymbol);

			int tsfPeriod = rs.getInt("tsfPeriod");
			double tsf[] = new double[gsdTSF.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.tsf(0, gsdTSF.inClose.length - 1, gsdTSF.inClose, tsfPeriod, outBegIdx, outNBElement, tsf);

			Realign.realign(tsf, outBegIdx.value);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");

			parms.addSymbol(symKey, rs.getInt("functionDaysDiff"), rs.getInt("doubleBack"), gsdTSF.dateIndex, tsf);

		}

		return parms;
	}

	@Override
	public String makeARFFFromSQL(String sym, int daysOut, AttributeParm parms, boolean startWithLargeGroup)
			throws Exception {

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		int pos = 25 + daysOut;
		String startDate = gsd.inDate[pos];

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_tsf_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);

		for (String symKey : parms.keySet()) {

			pw.println("@ATTRIBUTE " + symKey + "tsf NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "tsfBack NUMERIC");
			if (startDate.compareTo(parms.getDateIndex(symKey).firstKey()) < 0)
				startDate = parms.getDateIndex(symKey).firstKey();
		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");
		int lines = 0;
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;
		pos += 20;
		int largeGroupCnt = -1;
		int positiveCount = 0;
		int negativeCount = 0;
		skipDay: for (int npPos = pos; npPos < gsd.inDate.length - daysOut - 1; npPos++) {
			nextKey: for (String key : parms.keySet()) {

				Integer dateidx = parms.getDateIndex(key).get(gsd.inDate[npPos]);
				if (dateidx == null)
					continue skipDay;

				// check for missing days
				for (int i = 0; i < (parms.getDaysDiff(key) + daysOut) & (npPos + i < gsd.inDate.length); i++) {
					String gtest = gsd.inDate[npPos + i];
					if (parms.getDateIndex(key).containsKey(gtest) == false) {
						continue skipDay;
					}
				}

				continue nextKey;

			}
			if (gsd.inClose[npPos + daysOut] > gsd.inClose[npPos])
				positiveCount++;
			else
				negativeCount++;

		}
		boolean largeGroupIsPositive = positiveCount > negativeCount;
		int stopAt = 0;
		int startAt = 0;

		if (startWithLargeGroup) // start the first large group
			if (largeGroupIsPositive)
				stopAt = negativeCount;
			else
				stopAt = positiveCount;
		else { // start the second large group
			if (largeGroupIsPositive)
				startAt = positiveCount - negativeCount;
			else
				startAt = negativeCount - positiveCount;
		}
		for (int iday = pos; iday < gsd.inDate.length - daysOut - 1; iday++) {
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, parms, gsd.inClose, db,
					withAttributePosition, false);
			if (sb != null) {
				boolean positive = gsd.inClose[iday + daysOut] > gsd.inClose[iday];
				if (positive) {
					if (largeGroupIsPositive) {
						largeGroupCnt++;
						if (startWithLargeGroup) {
							if (largeGroupCnt > stopAt)
								continue;
						} else {
							if (largeGroupCnt < startAt)
								continue;
						}
					}
				} else {
					if (!largeGroupIsPositive) {
						largeGroupCnt++;
						if (startWithLargeGroup) {
							if (largeGroupCnt > stopAt)
								continue;
						} else {
							if (largeGroupCnt < startAt)
								continue;
						}
					}

				}

				pw.print(sb.toString());
				addDateToPosition(gsd.inDate[iday], lines);
				lines++;
			}
		}

		return sw.toString();
	}

	@Override
	public String makeARFFFromSQLForQuestionMark(String sym, int daysOut, AttributeParm parms) throws Exception {

		Core core = new Core();

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_tsf_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);

		for (String symKey : parms.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "tsf NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "tsfBack NUMERIC");
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

	public String getAttributeText(double tsf[], int tsffunctionDaysDiff, int tsfstart, int doubleBack) {

		return tsf[tsfstart - doubleBack] + "," + tsf[tsfstart - (tsffunctionDaysDiff + doubleBack)] + ",";

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
			double tsfs[] = ((TSFParms) parms).gettsfs(key);
			Integer doubleBack = parms.getDoubleBacks(key);
			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(tsfs, functionDaysDiff, dateidx, doubleBack.intValue()));
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
