package correlation.ARFFMaker;

import java.io.File;
import java.io.PrintWriter;
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
import correlation.ARFFMaker.Parms.AttributeParm;
import correlation.ARFFMaker.Parms.SMIParms;
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
		String data = sm.makeARFFFromSQL(sym, daysOut, parms, false);
		File file = new File(getFilename(sym, daysOut));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, int daysOut) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + daysOut + "_smi2bb_correlation.arff";
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
	public void printAttributeHeader(PrintWriter pw, String sym, int daysOut, DeltaBands db, AttributeParm parms,
			boolean allHeaderFields) {

		if (allHeaderFields) {
			pw.println("% 1. Title: " + sym + "_smi_correlation");
			pw.println("@RELATION " + sym + "_" + daysOut);
		}

		for (String symKey : parms.keySet()) {

			pw.println("@ATTRIBUTE " + symKey + "smi NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smi2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smi3 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal3 NUMERIC");

		}

		if (allHeaderFields) {
			if (withAttributePosition)
				pw.println("@ATTRIBUTE class NUMERIC");
			else
				pw.println(db.getAttributeDefinition());
		}

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
