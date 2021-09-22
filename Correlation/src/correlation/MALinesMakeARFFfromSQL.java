package correlation;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.americancoders.lineIntersect.Line;
import com.tictactec.ta.lib.MAType;

import bands.DeltaBands;
import movingAvgAndLines.MovingAvgAndLineIntercept;
import util.getDatabaseConnection;

public class MALinesMakeARFFfromSQL {

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
		malines.makeARFFFromSQL(sym, dos);
	}

	public String getFilename(String sym, String dos) {

		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_malis_correlation.arff";
	}

	public TreeMap<String, Integer> dateAttribute = new TreeMap<>();

	public void makeARFFFromSQL(String sym, String dos) throws Exception {

		Connection conn = null;

		int attributePos = -1;

		conn = getDatabaseConnection.makeConnection();

		PreparedStatement ps = conn
				.prepareStatement("select * from maline_correlation" + " where symbol=? and toCloseDays=?");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

		TreeMap<String, Object> malis = new TreeMap<>();
		TreeMap<String, String[]> malDates = new TreeMap<>();

		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		DeltaBands db = new DeltaBands(gsd.inClose, daysOut);
		int pos = 200;
		String startDate = gsd.inDate[pos];

		File file = new File(getFilename(sym, dos));
		PrintWriter pw = new PrintWriter(file);
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

			malis.put(symKey, mlp);

			pw.println("@ATTRIBUTE " + symKey + "maline1 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "maline2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "maline3 NUMERIC");
			malDates.put(symKey, pgsd.inDate);

		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int arraypos[] = new int[malis.size()];

		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		eemindexLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut - 1;) {
			String posDate = gsd.inDate[iday];
			pos = 0;
			for (String key : malis.keySet()) {
				String sdate = malDates.get(key)[arraypos[pos]];
				int dcomp = posDate.compareTo(sdate);
				if (dcomp < 0) {
					iday++;
					continue eemindexLoop;
				}
				if (dcomp > 0) {
					arraypos[pos]++;
					continue eemindexLoop;
				}
				pos++;
			}

			pos = 0;
			Date now = sdf.parse(posDate);

			Calendar cdMove = Calendar.getInstance();
			cdMove.setTime(now);
			int idt = 1;
			int daysOutCalc = 0;
			while (true) {
				cdMove.add(Calendar.DAY_OF_MONTH, +1);
				if (cdMove.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY)
					continue;
				if (cdMove.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
					continue;
				daysOutCalc++;
				idt++;
				if (idt > daysOut)
					break;
			}
			String endDate = gsd.inDate[iday + daysOut];
			dateAttribute.put(gsd.inDate[iday], ++attributePos);
			if (daysOutCalc != daysOut)
				System.out.println(posDate + " here " + endDate + " by " + daysOutCalc + " not " + daysOut);

			printAttributeData(iday, daysOut, pw, malis, arraypos, gsd.inClose, gsd.inDate[iday], db,
					withAttributePosition);

			iday++;
			for (pos = 0; pos < arraypos.length; pos++) {
				arraypos[pos]++;
			}
		}
		pw.close();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	protected void getAttributeText(PrintWriter pw, MaLineParmToPass ptp, int start) {

		Line ln, ln2, ln3;
		String rsp1 = "?", rsp2 = "?", rsp3 = "?";

		try {
			ln = ptp.mali.getCurrentLineIntercept(ptp.processDate, 1);
			double yln = ptp.closes[start] * ln.slope + ln.yintersect;
			rsp1 = yln + "";
		} catch (Exception e) {
			// TODO Auto-generated catch block

		}
		try {
			ln2 = ptp.mali.getCurrentLineIntercept(ptp.processDate, 2);
			double yln2 = ptp.closes[start] * ln2.slope + ln2.yintersect;
			rsp2 = yln2 + "";
		} catch (Exception e) {
			// TODO Auto-generated catch block

		}
		try {
			ln3 = ptp.mali.getCurrentLineIntercept(ptp.processDate, 3);
			double yln3 = ptp.closes[start] * ln3.slope + ln3.yintersect;
			rsp3 = yln3 + "";
		} catch (Exception e) {
			// TODO Auto-generated catch block

		}

		pw.print(rsp1 + "," + rsp2 + "," + rsp3 + ",");

	}

	public void printAttributeData(int iday, int daysOut, PrintWriter pw, TreeMap<String, Object> macds, int[] arraypos,
			double[] closes, String date, DeltaBands priceBands, boolean withAttributePosition) throws Exception {
		int pos = 0;
		for (String key : macds.keySet()) {

			MaLineParmToPass mlp = (MaLineParmToPass) macds.get(key);
			mlp.processDate = date;
			getAttributeText(pw, mlp, daysOut);

			pos++;

		}

		if (withAttributePosition)
			pw.println(priceBands.getAttributePosition(iday, daysOut, closes));
		else
			pw.println(priceBands.getAttributeValue(iday, daysOut, closes));
	}

}
