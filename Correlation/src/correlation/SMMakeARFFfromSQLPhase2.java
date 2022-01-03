package correlation;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.MAType;

import StochasticMomentum.StochasticMomentum;
import bands.DeltaBands;
import util.getDatabaseConnection;

public class SMMakeARFFfromSQLPhase2 {
	public class SMISymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		String[] dates;
		int lastDateStart;
		StochasticMomentum smis;
	}

	public class SMIParms implements AttributeParm {
		TreeMap<String, SMISymbolParm> SMIPMap;

		public SMIParms() {
			SMIPMap = new TreeMap<String, SMISymbolParm>();
		}

		@Override
		public Set<String> keySet() {
			return SMIPMap.keySet();
		}

		@Override
		public void addSymbol(String sym) {
			SMIPMap.put(sym, new SMISymbolParm());
			SMIPMap.get(sym).lastDateStart = 0;
		}

		@Override
		public Integer getDaysDiff(String sym) {
			return SMIPMap.get(sym).functionDaysDiff;
		}

		@Override
		public void setDaysDiff(String sym, Integer daysDiff) {
			SMIPMap.get(sym).functionDaysDiff = daysDiff;
		}

		@Override
		public Integer getDoubleBacks(String sym) {
			return SMIPMap.get(sym).doubleBacks;
		}

		@Override
		public void setDoubleBacks(String sym, Integer doubleBacks) {
			SMIPMap.get(sym).doubleBacks = doubleBacks;
		}

		public StochasticMomentum getSMIs(String sym) {
			return SMIPMap.get(sym).smis;
		}

		public void setSMIs(String sym, StochasticMomentum sm) {
			SMIPMap.get(sym).smis = sm;
		}

		@Override
		public String[] getAttrDates(String sym) {
			return SMIPMap.get(sym).dates;
		}

		@Override
		public void setAttrDates(String sym, String dates[]) {
			SMIPMap.get(sym).dates = dates;
		}

		@Override
		public int getLastDateStart(String sym) {
			return SMIPMap.get(sym).lastDateStart;
		}

		@Override
		public void setLastDateStart(String sym, int start) {
			SMIPMap.get(sym).lastDateStart = start;
		}

	}

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
	boolean makeWith30Days = true;

	public SMMakeARFFfromSQLPhase2(boolean b, boolean makeWith30) {
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
		SMMakeARFFfromSQLPhase2 sm = new SMMakeARFFfromSQLPhase2(false, true);

		String data = sm.makeARFFFromSQL(sym, dos, conn, false);
		File file = new File(getFilename(sym, dos));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, String daysOut) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + daysOut + "_smi2_correlation.arff";
	}

	public TreeMap<String, Integer> dateAttribute = new TreeMap<>();

	public String makeARFFFromSQL(String sym, String dos, Connection conn, boolean includeLastQuestionMark)
			throws Exception {

		PreparedStatement ps = conn.prepareStatement("select * from sm_correlation" + (makeWith30Days ? "" : "_180")
				+ " where symbol=?  and  toCloseDays=?");
		SMIParms smip = new SMIParms();
		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		int pos = 35 + daysOut;
		String startDate = gsd.inDate[pos];
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);

		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_smi_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(pgsd.inDate[pos]) < 0)
				startDate = pgsd.inDate[pos];

			int hiLowPeriod = rs.getInt("hiLowPeriod");
			int maPeriod = rs.getInt("maPeriod");
			int smSmoothPeriod = rs.getInt("smSmoothPeriod");
			int smSignalPeriod = rs.getInt("smSignalPeriod");
			StochasticMomentum sm = new StochasticMomentum(pgsd.inHigh, pgsd.inLow, pgsd.inClose, hiLowPeriod,
					MAType.Ema, maPeriod, smSmoothPeriod, smSignalPeriod);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");

			smip.addSymbol(symKey);
			smip.setSMIs(symKey, sm);
			smip.setDoubleBacks(symKey, rs.getInt("doubleBack"));
			smip.setDaysDiff(symKey, rs.getInt("functionDaysDiff"));
			smip.setAttrDates(symKey, pgsd.inDate);

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

		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		for (int iday = pos; iday < gsd.inDate.length - daysOut - 1; iday++) {
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, smip, gsd.inClose, db,
					withAttributePosition, false);
			if (sb != null)
				pw.print(sb.toString());
		}

		if (includeLastQuestionMark) {
			int iday = gsd.inDate.length - daysOut - 1;
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, smip, gsd.inClose, db,
					withAttributePosition, true);
			if (sb != null)
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

	public StringBuffer printAttributeData(int iday, String etfDates[], int daysOut, AttributeParm smip,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition, boolean forLastUseQuestionMark) {

		StringBuffer returnBuffer = new StringBuffer(1000);
		for (String key : smip.keySet()) {
			String dates[] = smip.getAttrDates(key);
			int smistart = smip.getLastDateStart(key);
			int smifunctionDaysDiff = smip.getDaysDiff(key);

			for (; smistart < dates.length; smistart++) {
				if (dates[smistart].compareTo(etfDates[iday]) == 0)
					break;
				if (dates[smistart].compareTo(etfDates[iday]) > 0)
					return null;
			}
			// are we missing any days in between
			int giDay = iday - smifunctionDaysDiff;
			int siDay = smistart - smifunctionDaysDiff;
			// are we missing any days in between
			for (int i = 0; i < (smifunctionDaysDiff + daysOut); i++) {
				String gtest = etfDates[giDay + i];
				String stest = dates[siDay + i];
				if (gtest.compareTo(stest) != 0) {
					return null;
				}
			}

			smip.setLastDateStart(key, smistart);
			StochasticMomentum smis = ((SMIParms) smip).getSMIs(key);
			Integer doubleBack = smip.getDoubleBacks(key);
			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(smis, smifunctionDaysDiff, smistart, daysOut, doubleBack.intValue()));
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
