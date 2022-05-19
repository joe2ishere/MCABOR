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
import com.americancoders.lineIntersect.Line;
import com.americancoders.lineIntersect.Point;
import com.tictactec.ta.lib.MAType;

import bands.DeltaBands;
import correlation.ARFFMaker.Parms.AttributeParm;
import correlation.ARFFMaker.Parms.MAAvgParms;
import correlation.ARFFMaker.Parms.MaLineParmToPass;
import movingAvgAndLines.MovingAvgAndLineIntercept;
import util.getDatabaseConnection;

public class MAAveragesMakeARFFfromSQL extends AttributeMakerFromSQL {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5

	public MAAveragesMakeARFFfromSQL(boolean b) {
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
		MAAveragesMakeARFFfromSQL malines = new MAAveragesMakeARFFfromSQL(false);
		Connection conn = getDatabaseConnection.makeConnection();
		int daysOut = Integer.parseInt(dos);
		AttributeParm parms = malines.buildParameters(sym, daysOut, conn);
		String data = malines.makeARFFFromSQL(sym, daysOut, parms, false);
		File file = new File(getFilename(sym, daysOut));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, int dos) {

		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_malavg2bb_correlation.arff";
	}

	@Override
	public AttributeParm buildParameters(String sym, int daysOut, Connection conn) throws Exception {
		MAAvgParms parms = new MAAvgParms();

		PreparedStatement ps = conn
				.prepareStatement("select * from maline_correlation" + " where symbol=? and toCloseDays=?");

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
	public void printAttributeHeader(PrintWriter pw, String sym, int daysOut, DeltaBands db, AttributeParm parms,
			boolean allHeaderFields) {

		if (allHeaderFields) {
			pw.println("% 1. Title: " + sym + "_malis_correlation");
			pw.println("@RELATION " + sym + "_" + daysOut);
		}

		for (String symKey : parms.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "maAVG NUMERIC");
		}

		if (allHeaderFields) {
			if (withAttributePosition)
				pw.println("@ATTRIBUTE class NUMERIC");
			else
				pw.println(db.getAttributeDefinition());
		}
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public void getAttributeText(StringBuffer sb, MaLineParmToPass ptp, int start) {

		Line ln;
		String rsp1 = "?";
		try {
			Point pt0 = ptp.mali.getPoint(ptp.processDate, 0);
			Point pt1 = ptp.mali.getPoint(ptp.processDate, 1);
			Point pt2 = ptp.mali.getPoint(ptp.processDate, 2);
			Point pt3 = ptp.mali.getPoint(ptp.processDate, 3);
			Point ptavg = new Point((pt1.xPoint + pt2.xPoint + pt3.xPoint) / 3,
					(pt1.yPoint + pt2.yPoint + pt3.yPoint) / 3);
			ln = new Line(ptavg, pt0);
			Double xp = pt0.xPoint;
			double yln = ptp.closes[xp.intValue()] / (((pt0.xPoint + start) * ln.slope) + ln.yintersect);
			rsp1 = yln + "";
		} catch (Exception e) {
		}
		sb.append(rsp1 + ",");
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
