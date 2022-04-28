package correlation;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import StochasticMomentum.StochasticMomentum;
import bands.DeltaBands;
import correlation.SMMakeARFFfromSQL.SMIParms;
import util.getDatabaseConnection;

public class SMIandTSFMakeARFFfromSQL extends AttributeMakerFromSQL {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5

	public SMIandTSFMakeARFFfromSQL(boolean b, boolean makeWith30) {
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

		SMIandTSFMakeARFFfromSQL ma = new SMIandTSFMakeARFFfromSQL(false, true);

		int daysOut = Integer.parseInt(dos);

		String data = ma.makeARFFFromSQL(sym, daysOut, true);

		File file = new File(getFilename(sym, daysOut));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, int dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_smiAndtsf_correlation.arff";
	}

	@Override
	public AttributeParm buildParameters(String sym, int daysOut, Connection conn) throws Exception {

		return null;
	}

	public String makeARFFFromSQL(String sym, int daysOut, AttributeParm smiParm, AttributeParm tsfParm,
			boolean startWithLargeGroup) throws Exception {

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		int pos = 25 + daysOut;
		String startDate = gsd.inDate[pos];
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_smi_tsf_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);
		for (String symKey : smiParm.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "smi NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smisignal NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smi2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smisignal2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smi3 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smisignal3 NUMERIC");
			if (startDate.compareTo(smiParm.getDateIndex(symKey).firstKey()) < 0)
				startDate = smiParm.getDateIndex(symKey).firstKey();
		}
		for (String symKey : tsfParm.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "tsf NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "tsfBack NUMERIC");
			if (startDate.compareTo(tsfParm.getDateIndex(symKey).firstKey()) < 0)
				startDate = tsfParm.getDateIndex(symKey).firstKey();
		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;
		pos += 20;

		int largeGroupCnt = -1;
		int positiveCount = 0;
		int negativeCount = 0;

		skipDay: for (int npPos = pos; npPos < gsd.inDate.length - daysOut - 1; npPos++) {
			nextKey: for (String key : smiParm.keySet()) {

				Integer dateidx = smiParm.getDateIndex(key).get(gsd.inDate[npPos]);
				if (dateidx == null)
					continue skipDay;

				// check for missing days
				for (int i = 0; i < (smiParm.getDaysDiff(key) + daysOut) & (npPos + i < gsd.inDate.length); i++) {
					String gtest = gsd.inDate[npPos + i];
					if (smiParm.getDateIndex(key).containsKey(gtest) == false) {
						continue skipDay;
					}
				}

				continue nextKey;
			}

			nextKey2: for (String key : tsfParm.keySet()) {

				Integer dateidx = tsfParm.getDateIndex(key).get(gsd.inDate[npPos]);
				if (dateidx == null)
					continue skipDay;

				// check for missing days
				for (int i = 0; i < (tsfParm.getDaysDiff(key) + daysOut) & (npPos + i < gsd.inDate.length); i++) {
					String gtest = gsd.inDate[npPos + i];
					if (tsfParm.getDateIndex(key).containsKey(gtest) == false) {
						continue skipDay;
					}
				}

				dateidx = tsfParm.getDateIndex(key).get(gsd.inDate[npPos]);
				if (dateidx == null)
					continue skipDay;

				// check for missing days
				for (int i = 0; i < (tsfParm.getDaysDiff(key) + daysOut) & (npPos + i < gsd.inDate.length); i++) {
					String gtest = gsd.inDate[npPos + i];
					if (tsfParm.getDateIndex(key).containsKey(gtest) == false) {
						continue skipDay;
					}
				}

				continue nextKey2;
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
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, smiParm, tsfParm, gsd.inClose, db,
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

			}
		}

		return sw.toString();
	}

	public String makeARFFFromSQL(String sym, int daysOut, boolean startWithLargeGroup) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();
		SMMakeARFFfromSQL smi = new SMMakeARFFfromSQL(withAttributePosition, startWithLargeGroup);
		AttributeParm smiParm = smi.buildParameters(sym, daysOut, conn);
		TSFMakeARFFfromSQL tsf = new TSFMakeARFFfromSQL(withAttributePosition, startWithLargeGroup);
		AttributeParm tsfParm = tsf.buildParameters(sym, daysOut, conn);
		return makeARFFFromSQL(sym, daysOut, smiParm, tsfParm, startWithLargeGroup);
	}

	public String makeARFFFromSQLForQuestionMark(String sym, int daysOut, AttributeParm smiParm, AttributeParm tsfParm)
			throws Exception {

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_smi_tsf_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);
		for (String symKey : smiParm.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "smi NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smisignal NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smi2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smisignal2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smi3 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "smisignal3 NUMERIC");

		}
		for (String symKey : tsfParm.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "tsf NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "tsfBack NUMERIC");
		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int iday = gsd.inDate.length - 1;
		StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, smiParm, tsfParm, gsd.inClose, db,
				withAttributePosition, true);
		if (sb != null) {
			pw.print(sb.toString());

		}

		return sw.toString();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(double tsf[], int tsffunctionDaysDiff, int tsfstart, int doubleBack) {

		return tsf[tsfstart - doubleBack] + "," + tsf[tsfstart - (tsffunctionDaysDiff + doubleBack)] + ",";

	}

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

	public StringBuffer printAttributeData(int iday, String etfDates[], int daysOut, AttributeParm smiParms,
			AttributeParm tsfParms, double[] closes, DeltaBands priceBands, boolean withAttributePosition,
			boolean forLastUseQuestionMark) {
		StringBuffer returnBuffer = new StringBuffer(100);
		for (String key : smiParms.keySet()) {

			int functionDaysDiff = smiParms.getDaysDiff(key);
			Integer dateidx = smiParms.getDateIndex(key).get(etfDates[iday]);
			if (dateidx == null)
				return null;

			// are we missing any days in between
			int giDay = iday - functionDaysDiff;

			// are we missing any days in between
			if (!forLastUseQuestionMark)
				for (int i = 0; i < (functionDaysDiff + daysOut); i++) {
					String gtest = etfDates[giDay + i];
					if (smiParms.getDateIndex(key).containsKey(gtest) == false) {
						return null;
					}
				}
			StochasticMomentum smis = ((SMIParms) smiParms).getSMIs(key);
			Integer doubleBack = smiParms.getDoubleBacks(key);
			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(smis, functionDaysDiff, dateidx, daysOut, doubleBack.intValue()));
		}
		for (String key : tsfParms.keySet()) {
			int functionDaysDiff = tsfParms.getDaysDiff(key);
			Integer dateidx = tsfParms.getDateIndex(key).get(etfDates[iday]);
			if (dateidx == null)
				return null;

			// are we missing any days in between
			int giDay = iday - functionDaysDiff;

			// are we missing any days in between
			if (!forLastUseQuestionMark)
				for (int i = 0; i < (functionDaysDiff + daysOut); i++) {
					String gtest = etfDates[giDay + i];
					if (tsfParms.getDateIndex(key).containsKey(gtest) == false) {
						return null;
					}
				}

			double tsfs[] = ((TSFParms) tsfParms).gettsfs(key);
			Integer doubleBack = tsfParms.getDoubleBacks(key);
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
