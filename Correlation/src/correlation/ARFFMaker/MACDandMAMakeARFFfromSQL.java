package correlation.ARFFMaker;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import bands.DeltaBands;
import correlation.ARFFMaker.Parms.AttributeParm;
import correlation.ARFFMaker.Parms.MACDParms;
import correlation.ARFFMaker.Parms.MAParms;
import util.getDatabaseConnection;

public class MACDandMAMakeARFFfromSQL extends AttributeMakerFromSQL {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5

	public MACDandMAMakeARFFfromSQL(boolean b, boolean makeWith30) {
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

		MACDandMAMakeARFFfromSQL ma = new MACDandMAMakeARFFfromSQL(false, true);

		int daysOut = Integer.parseInt(dos);

		String data = ma.makeARFFFromSQL(sym, daysOut, true);
		File file = new File(getFilename(sym, daysOut));
		PrintWriter pw = new PrintWriter(file);
		pw.print(data);
		pw.flush();
		pw.close();
	}

	public static String getFilename(String sym, int dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_macdAndma_correlation.arff";
	}

	@Override
	public AttributeParm buildParameters(String sym, int daysOut, Connection conn) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	public String makeARFFFromSQL(String sym, int daysOut, AttributeParm macdParm, AttributeParm maParm,
			boolean startWithLargeGroup) throws Exception {

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		int pos = 25 + daysOut;
		String startDate = gsd.inDate[pos];
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_macd_ma_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);
		for (String symKey : macdParm.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "ma NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			if (startDate.compareTo(macdParm.getDateIndex(symKey).firstKey()) < 0)
				startDate = macdParm.getDateIndex(symKey).firstKey();
		}
		for (String symKey : maParm.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "ma2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal2 NUMERIC");
			if (startDate.compareTo(maParm.getDateIndex(symKey).firstKey()) < 0)
				startDate = maParm.getDateIndex(symKey).firstKey();
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
			nextKey: for (String key : macdParm.keySet()) {

				Integer dateidx = macdParm.getDateIndex(key).get(gsd.inDate[npPos]);
				if (dateidx == null)
					continue skipDay;

				// check for missing days
				for (int i = 0; i < (macdParm.getDaysDiff(key) + daysOut) & (npPos + i < gsd.inDate.length); i++) {
					String gtest = gsd.inDate[npPos + i];
					if (macdParm.getDateIndex(key).containsKey(gtest) == false) {
						continue skipDay;
					}
				}

				continue nextKey;
			}
			nextKey2: for (String key : maParm.keySet()) {

				Integer dateidx = maParm.getDateIndex(key).get(gsd.inDate[npPos]);
				if (dateidx == null)
					continue skipDay;

				// check for missing days
				for (int i = 0; i < (maParm.getDaysDiff(key) + daysOut) & (npPos + i < gsd.inDate.length); i++) {
					String gtest = gsd.inDate[npPos + i];
					if (maParm.getDateIndex(key).containsKey(gtest) == false) {
						continue skipDay;
					}
				}

				dateidx = maParm.getDateIndex(key).get(gsd.inDate[npPos]);
				if (dateidx == null)
					continue skipDay;

				// check for missing days
				for (int i = 0; i < (maParm.getDaysDiff(key) + daysOut) & (npPos + i < gsd.inDate.length); i++) {
					String gtest = gsd.inDate[npPos + i];
					if (maParm.getDateIndex(key).containsKey(gtest) == false) {
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
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, macdParm, maParm, gsd.inClose, db,
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
		MACDMakeARFFfromSQL macd = new MACDMakeARFFfromSQL(withAttributePosition, startWithLargeGroup);
		AttributeParm macdParm = macd.buildParameters(sym, daysOut, conn);
		MAMakeARFFfromSQL ma = new MAMakeARFFfromSQL(withAttributePosition, startWithLargeGroup);
		AttributeParm maParm = ma.buildParameters(sym, daysOut, conn);

		return makeARFFFromSQL(sym, daysOut, macdParm, maParm, startWithLargeGroup);
	}

	public String makeARFFFromSQLForQuestionMark(String sym, int daysOut, AttributeParm macdParms,
			AttributeParm maParms) throws Exception {

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_ma_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);
		for (String symKey : macdParms.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "ma NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
		}
		for (String symKey : maParms.keySet()) {
			pw.println("@ATTRIBUTE " + symKey + "ma2 NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal2 NUMERIC");
		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int iday = gsd.inDate.length - 1;
		StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, macdParms, maParms, gsd.inClose, db,
				withAttributePosition, true);
		if (sb != null) {
			pw.print(sb.toString());

		}

		return sw.toString();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(double[] macd, double[] signal, int macdfunctionDaysDiff, int macdstart,
			int doubleBack) {
		return macd[macdstart] + "," /* + signal[macdstart - doubleBack] + "," */
				+ macd[macdstart - doubleBack] + "," /* + signal[macdstart - doubleBack] + "," */;

	}

	public String getAttributeText(double ma[], int mafunctionDaysDiff, int mastart, int doubleBack) {
		return ma[mastart - doubleBack] + "," + ma[mastart - (mafunctionDaysDiff + doubleBack)] + ",";

	}

	public StringBuffer printAttributeData(int iday, String etfDates[], int daysOut, AttributeParm macdParms,
			AttributeParm maParms, double[] closes, DeltaBands priceBands, boolean withAttributePosition,
			boolean forLastUseQuestionMark) {
		StringBuffer returnBuffer = new StringBuffer(100);

		for (String key : macdParms.keySet()) {
			int functionDaysDiff = macdParms.getDaysDiff(key);
			Integer dateidx = macdParms.getDateIndex(key).get(etfDates[iday]);
			if (dateidx == null)
				return null;

			// are we missing any days in between
			int giDay = iday - functionDaysDiff;

			// are we missing any days in between
			if (!forLastUseQuestionMark)
				for (int i = 0; i < (functionDaysDiff + daysOut); i++) {
					String gtest = etfDates[giDay + i];
					if (macdParms.getDateIndex(key).containsKey(gtest) == false) {
						return null;
					}
				}

			double[] macd = ((MACDParms) macdParms).getMACD(key);
			double[] signal = ((MACDParms) macdParms).getSignal(key);

			Integer doubleBack = macdParms.getDoubleBacks(key);
			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(macd, signal, functionDaysDiff, dateidx, doubleBack.intValue()));
		}

		for (String key : maParms.keySet()) {

			int functionDaysDiff = maParms.getDaysDiff(key);
			Integer dateidx = maParms.getDateIndex(key).get(etfDates[iday]);
			if (dateidx == null)
				return null;

			// are we missing any days in between
			int giDay = iday - functionDaysDiff;

			// are we missing any days in between
			if (!forLastUseQuestionMark)
				for (int i = 0; i < (functionDaysDiff + daysOut); i++) {
					String gtest = etfDates[giDay + i];
					if (maParms.getDateIndex(key).containsKey(gtest) == false) {
						return null;
					}
				}
			double mas[] = ((MAParms) maParms).getMAs(key);
			Integer doubleBack = maParms.getDoubleBacks(key);
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

	@Override
	public StringBuffer printAttributeData(int iday, String[] inDate, int daysOut, AttributeParm parms,
			double[] inClose, DeltaBands priceBands, boolean withAttributePosition, boolean forLastUseQuestionMark) {
		// TODO Auto-generated method stub
		return null;
	}

}
