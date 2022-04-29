package correlation;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.MAType;

import StochasticMomentum.StochasticMomentum;
import bands.DeltaBands;
import util.getDatabaseConnection;

public class SMMakeARFFfromSQL extends AttributeMakerFromSQL {
	public record SMISymbolParm(Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			StochasticMomentum smis) {

	}

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
	boolean makeWith30Days = true;

	public SMMakeARFFfromSQL(boolean b, boolean makeWith30) {
		withAttributePosition = b;
		makeWith30Days = makeWith30;
	}

	public static void main(String[] args) throws Exception {

		LogManager.getLogManager().reset();
		Connection conn = getDatabaseConnection.makeConnection();
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
		SMMakeARFFfromSQL sm = new SMMakeARFFfromSQL(false, true);
		int daysOut = Integer.parseInt(dos);
		AttributeParm parms = sm.buildParameters(sym, daysOut, conn);
		String data = sm.makeARFFFromSQL(sym, daysOut, parms, true);
		File file = new File(getFilename(sym, daysOut));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, int daysOut) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + daysOut + "_smi2_correlation.arff";
	}

	@Override
	public AttributeParm buildParameters(String sym, int daysOut, Connection conn) throws Exception {
		SMIParms parms = new SMIParms();

		PreparedStatement ps = conn.prepareStatement("select * from sm_correlation" + (makeWith30Days ? "" : "_130")
				+ " where symbol=?  and  toCloseDays=?");

		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(functionSymbol);

			int hiLowPeriod = rs.getInt("hiLowPeriod");
			int maPeriod = rs.getInt("maPeriod");
			int smSmoothPeriod = rs.getInt("smSmoothPeriod");
			int smSignalPeriod = rs.getInt("smSignalPeriod");
			StochasticMomentum sm = new StochasticMomentum(pgsd.inHigh, pgsd.inLow, pgsd.inClose, hiLowPeriod,
					MAType.Ema, maPeriod, smSmoothPeriod, smSignalPeriod);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");

			parms.addSymbol(symKey, rs.getInt("functionDaysDiff"), rs.getInt("doubleBack"), pgsd.dateIndex, sm);

		}

		return parms;
	}

	@Override
	public String makeARFFFromSQL(String sym, int daysOut, AttributeParm parms, boolean startWithLargeGroup)
			throws Exception {

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

		int pos = 35 + daysOut;
		String startDate = gsd.inDate[pos];
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_smi_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);

		for (String symKey : parms.keySet()) {

			pw.println("@ATTRIBUTE " + symKey + "smi NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smi2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smi3 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal3 NUMERIC");
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

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

		int pos = 35 + daysOut;

		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_smi_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);

		for (String symKey : parms.keySet()) {

			pw.println("@ATTRIBUTE " + symKey + "smi NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smi2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smi3 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal3 NUMERIC");

		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");
		int iday = gsd.inDate.length - daysOut - 1;
		StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, parms, gsd.inClose, db, withAttributePosition,
				true);
		if (sb != null) {
			pw.print(sb.toString());

		}
		return sw.toString();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(StochasticMomentum sm, int smfunctionDaysDiff, int smstart, int daysOut,
			int doubleBack) {
		String ret = "";
		int dorel = daysOut / 5;
		dorel *= 2;
		ret += sm.SMI[smstart - ((dorel) + doubleBack)];
		ret += ",";
		ret += sm.SMI[smstart - ((dorel) + doubleBack + 1)];
		ret += ",";
		ret += sm.SMI[smstart - ((dorel) + doubleBack + 2)];
		ret += ",";
		ret += sm.Signal[smstart - ((dorel) + doubleBack)];
		ret += ",";
		ret += sm.Signal[smstart - ((dorel) + doubleBack + 1)];
		ret += ",";
		ret += sm.Signal[smstart - ((dorel) + doubleBack + 2)];
		ret += ",";

		return ret;
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

			StochasticMomentum smis = ((SMIParms) parms).getSMIs(key);
			Integer doubleBack = parms.getDoubleBacks(key);
			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(smis, functionDaysDiff, dateidx, daysOut, doubleBack.intValue()));
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
