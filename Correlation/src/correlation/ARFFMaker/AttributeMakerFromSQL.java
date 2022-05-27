package correlation.ARFFMaker;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import bands.DeltaBands;
import correlation.ARFFMaker.Parms.AttributeParm;

public abstract class AttributeMakerFromSQL {
	public TreeMap<String, Integer> dateAttributeLookUp = new TreeMap<>();
	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5

	enum whichGroup {
		BEGIN, MIDDLE, END
	};

	public void addDateToPosition(String date, int position) {
		dateAttributeLookUp.put(date, position);
	}

	public Integer getDatePosition(String date) {

		return dateAttributeLookUp.get(date);
	}

	public String makeARFFFromSQL(String sym, int daysOut, Connection conn) throws Exception {

		AttributeParm parms = buildParameters(sym, daysOut, conn);
		return makeARFFFromSQL(sym, daysOut, parms, false);
	}

	public abstract AttributeParm buildParameters(String sym, int daysOut, Connection conn) throws Exception;

	public String makeARFFFromSQL(String sym, int daysOut, AttributeParm parms, boolean startWithLargeGroup)
			throws Exception {

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5, startWithLargeGroup);

		/*
		 * TreeMap<String, Integer> counters = new TreeMap<>(); for (int i = 0; i <
		 * gsd.inClose.length - daysOut; i++) { String got = (db.getAttributeValue(i,
		 * daysOut, gsd.inClose)); Integer gotI = counters.get(got); if (gotI == null) {
		 * gotI = 0; counters.put(got, gotI); } gotI++; counters.replace(got, gotI); }
		 * for (String key : counters.keySet()) { System.out.println(key + ";" +
		 * counters.get(key)); }
		 */

		int pos = 25 + daysOut;
		String startDate = setStartDate(gsd.inDate[pos], parms);
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);

		printAttributeHeader(pw, sym, daysOut, db, parms, true);

		pw.println("@DATA");

		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;
		pos += 20;
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

	public String makeARFFFromSQL(String sym, int daysOut, AttributeParm parms, whichGroup whichgroup)
			throws Exception {
		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		int pos = 25 + daysOut;
		String startDate = setStartDate(gsd.inDate[pos], parms);
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);

		printAttributeHeader(pw, sym, daysOut, db, parms, true);

		pw.println("@DATA");

		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;
		pos += 20;
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

		switch (whichgroup) {
		case BEGIN:
			if (largeGroupIsPositive)
				stopAt = negativeCount;
			else
				stopAt = positiveCount;

			break;

		case MIDDLE:
			if (largeGroupIsPositive)
				startAt = positiveCount - negativeCount;
			else
				startAt = negativeCount - positiveCount;
			startAt /= 2;
			if (largeGroupIsPositive)
				stopAt = negativeCount + startAt;
			else
				stopAt = positiveCount + startAt;
			break;

		case END:
			if (largeGroupIsPositive)
				startAt = positiveCount - negativeCount;
			else
				startAt = negativeCount - positiveCount;

			break;

		}
		for (int iday = pos; iday < gsd.inDate.length - daysOut - 1; iday++) {
			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, parms, gsd.inClose, db,
					withAttributePosition, false);
			if (sb != null) {
				boolean positive = gsd.inClose[iday + daysOut] > gsd.inClose[iday];
				if (positive) {
					if (largeGroupIsPositive) {
						largeGroupCnt++;
						if (whichgroup == whichGroup.BEGIN) {
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
						if (whichgroup == whichGroup.BEGIN) {
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

	public String makeARFFFromSQLForQuestionMark(String sym, int daysOut, AttributeParm parms) throws Exception {

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);

		printAttributeHeader(pw, sym, daysOut, db, parms, true);

		pw.println("@DATA");

		int iday = gsd.inDate.length - 1;
		StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, parms, gsd.inClose, db, withAttributePosition,
				true);
		if (sb != null) {
			pw.print(sb.toString());

		}

		return sw.toString();
	}

	public String makeARFFFromSQLForQuestionMark(String sym, int daysOut, AttributeParm parms, AttributeParm parms2)
			throws Exception {
		return null;
	}

	public abstract StringBuffer printAttributeData(int iday, String[] inDate, int daysOut, AttributeParm parms,
			double[] inClose, DeltaBands priceBands, boolean withAttributePosition, boolean forLastUseQuestionMark);

	public String setStartDate(String inStartDate, AttributeParm parms) {
		String startDate = inStartDate;
		for (String symKey : parms.keySet()) {
			if (startDate.compareTo(parms.getDateIndex(symKey).firstKey()) < 0)
				startDate = parms.getDateIndex(symKey).firstKey();
		}
		return startDate;
	}

	public String makeARFFFromSQL(String sym, int daysOut, AttributeParm parms, AttributeParm parms2,
			boolean startWithLargeGroup) throws Exception {
		return null;
	}

	public static String getFilename(String sym, int dos) {
		return null;
	}

	public void printAttributeHeader(PrintWriter pw, String sym, int daysOut, DeltaBands db, AttributeParm parms,
			boolean allHeaderFields) {

	}

}
