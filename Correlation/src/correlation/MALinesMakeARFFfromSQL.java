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
import com.americancoders.lineIntersect.Line;
import com.americancoders.lineIntersect.Point;
import com.tictactec.ta.lib.MAType;

import bands.DeltaBands;
import movingAvgAndLines.MovingAvgAndLineIntercept;
import util.getDatabaseConnection;

public class MALinesMakeARFFfromSQL extends AttributeMakerFromSQL {
	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5

	public MALinesMakeARFFfromSQL(boolean b) {
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
		MALinesMakeARFFfromSQL malines = new MALinesMakeARFFfromSQL(false);
		Connection conn = getDatabaseConnection.makeConnection();
		int daysOut = Integer.parseInt(dos);
		AttributeParm parms = malines.buildParameters(sym, daysOut, conn);
		String data = malines.makeARFFFromSQL(sym, daysOut, parms, true);
		File file = new File(getFilename(sym, daysOut));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, int dos) {

		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_malis2_correlation.arff";
	}

	@Override
	public AttributeParm buildParameters(String sym, int daysOut, Connection conn) throws Exception {
		MAAvgParms parms = new MAAvgParms();

		PreparedStatement ps = conn
				.prepareStatement("select * from maline_correlation" + " where symbol=? and toCloseDays=?");

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");
			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(functionSymbol);

			int period = rs.getInt("period");
			String matype = rs.getString("maType");
			MAType maType = null;
			for (MAType types : MAType.values()) {
				if (types.name().compareTo(matype) == 0) {
					maType = types;
					break;
				}
			}

			MovingAvgAndLineIntercept mal = new MovingAvgAndLineIntercept(pgsd, period, maType, period, maType);
			MaLineParmToPass mlp = new MaLineParmToPass(mal, pgsd.inClose, null);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");

			parms.addSymbol(symKey, 0, 0, pgsd.dateIndex, mlp);

		}

		return parms;

	}

	@Override
	public String makeARFFFromSQL(String sym, int daysOut, AttributeParm parms, boolean startWithLargeGroup)
			throws Exception {

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

		DeltaBands db = new DeltaBands(gsd.inClose, daysOut);
		int pos = 200;
		String startDate = gsd.inDate[pos];

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_malis_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);

		for (String symKey : parms.keySet()) {

			pw.println("@ATTRIBUTE " + symKey + "maline1 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "maline2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "maline3 NUMERIC");
			if (startDate.compareTo(parms.getDateIndex(symKey).firstKey()) < 0)
				startDate = parms.getDateIndex(symKey).firstKey();
		}

		pos = gsd.dateIndex.get(startDate) + 5;

		startDate = gsd.getInDate()[pos];
		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");
		int lines = 0;
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

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

		DeltaBands db = new DeltaBands(gsd.inClose, daysOut);
		int pos = 200;

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_malis_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);

		for (String symKey : parms.keySet()) {

			pw.println("@ATTRIBUTE " + symKey + "maline1 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "maline2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "maline3 NUMERIC");

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

	public void getAttributeText(StringBuffer sb, MaLineParmToPass ptp, int start) {

		Line ln, ln2, ln3;
		String rsp1 = "?", rsp2 = "?", rsp3 = "?";
		try {
			Point pt0 = ptp.mali.getPoint(ptp.processDate, 0);
			Point pt1 = ptp.mali.getPoint(ptp.processDate, 1);
			ln = new Line(pt1, pt0);
			Point pt2 = ptp.mali.getPoint(ptp.processDate, 2);
			ln2 = new Line(pt2, pt0);
			Point pt3 = ptp.mali.getPoint(ptp.processDate, 3);
			ln3 = new Line(pt3, pt0);
			Double xp = pt0.xPoint;
			ln = ptp.mali.getCurrentLineIntercept(ptp.processDate, 1);
			double yln = ptp.closes[xp.intValue()] / ((pt0.xPoint + start) * ln.slope + ln.yintersect);
			rsp1 = yln + "";
			ln2 = ptp.mali.getCurrentLineIntercept(ptp.processDate, 2);
			double yln2 = ptp.closes[xp.intValue()] / ((pt0.xPoint + start) * ln2.slope + ln2.yintersect);
			rsp2 = yln2 + "";
			ln3 = ptp.mali.getCurrentLineIntercept(ptp.processDate, 3);
			double yln3 = ptp.closes[xp.intValue()] / ((pt0.xPoint + start) * ln3.slope + ln3.yintersect);
			rsp3 = yln3 + "";
		} catch (Exception e) {

		}
		sb.append(rsp1 + "," + rsp2 + "," + rsp3 + ",");

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

			MaLineParmToPass mlp = ((MAAvgParms) parms).getMALI(key);
			mlp.processDate = etfDates[iday];
			getAttributeText(returnBuffer, mlp, daysOut);
			if (returnBuffer == null)
				return new StringBuffer();
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
