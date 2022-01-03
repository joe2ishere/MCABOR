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

public class MAAveragesMakeARFFfromSQLPhase2 {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5

	public MAAveragesMakeARFFfromSQLPhase2(boolean b) {
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
		MAAveragesMakeARFFfromSQLPhase2 malines = new MAAveragesMakeARFFfromSQLPhase2(false);
		Connection conn = getDatabaseConnection.makeConnection();
		String data = malines.makeARFFFromSQL(sym, dos, conn, false);
		File file = new File(getFilename(sym, dos));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, String dos) {

		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_malavg2_correlation.arff";
	}

	public String makeARFFFromSQL(String sym, String dos, Connection conn, boolean includeLastQuestionMark)
			throws Exception {

		MAAvgParms maavgp = new MAAvgParms();

		PreparedStatement ps = conn
				.prepareStatement("select * from maline_correlation" + " where symbol=? and toCloseDays=?");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		DeltaBands db = new DeltaBands(gsd.inClose, daysOut);
		int pos = 200;
		String startDate = gsd.inDate[pos];

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_malis_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");
			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(functionSymbol);
			if (startDate.compareTo(pgsd.inDate[pos]) < 0)
				startDate = pgsd.inDate[pos];
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

			maavgp.addSymbol(symKey);
			maavgp.setMALI(symKey, mlp);
			maavgp.setAttrDates(symKey, pgsd.inDate);

			pw.println("@ATTRIBUTE " + symKey + "maAVG NUMERIC");

		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		for (int iday = pos; iday < gsd.inDate.length - daysOut - 1; iday++) {
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, maavgp, gsd.inClose, db,
					withAttributePosition, false);
			if (sb != null)
				pw.print(sb.toString());
		}

		if (includeLastQuestionMark) {
			int iday = gsd.inDate.length - 1;
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, maavgp, gsd.inClose, db,
					withAttributePosition, true);
			if (sb != null)
				pw.print(sb.toString());
		}

		return sw.toString();
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

	public StringBuffer printAttributeData(int iday, String etfDates[], int daysOut, AttributeParm maavgp,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition, boolean forLastUseQuestionMark) {
		StringBuffer returnBuffer = new StringBuffer(1000);
		for (String key : maavgp.keySet()) {
			String dates[] = maavgp.getAttrDates(key);
			int maavgstart = maavgp.getLastDateStart(key);
			for (; maavgstart < dates.length; maavgstart++) {
				if (dates[maavgstart].compareTo(etfDates[iday]) == 0)
					break;
				if (dates[maavgstart].compareTo(etfDates[iday]) > 0)
					return null;
			}
			maavgp.setLastDateStart(key, maavgstart);
			MaLineParmToPass mlp = ((MAAvgParms) maavgp).getMALI(key);
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
