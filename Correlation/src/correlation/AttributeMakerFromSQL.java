package correlation;

import java.sql.Connection;
import java.util.TreeMap;

public abstract class AttributeMakerFromSQL {
	public TreeMap<String, Integer> dateAttributeLookUp = new TreeMap<>();

	public void addDateToPosition(String date, int position) {
		dateAttributeLookUp.put(date, position);
	}

	public Integer getDatePosition(String date) {

		return dateAttributeLookUp.get(date);
	}

	public String makeARFFFromSQL(String sym, int daysOut, Connection conn) throws Exception {

		AttributeParm parms = buildParameters(sym, daysOut, conn);
		return makeARFFFromSQL(sym, daysOut, parms, true);
	}

	public abstract AttributeParm buildParameters(String sym, int daysOut, Connection conn) throws Exception;

	public String makeARFFFromSQLForQuestionMark(String sym, int daysOut, AttributeParm parms) throws Exception {
		return null;
	}

	public String makeARFFFromSQLForQuestionMark(String sym, int daysOut, AttributeParm parms, AttributeParm parms2)
			throws Exception {
		return null;
	}

	public String makeARFFFromSQL(String sym, int daysOut, AttributeParm parms, boolean startWithLargeGroup)
			throws Exception {
		return null;
	}

	public String makeARFFFromSQL(String sym, int daysOut, AttributeParm parms, AttributeParm parms2,
			boolean startWithLargeGroup) throws Exception {
		return null;
	}

	public static String getFilename(String sym, int dos) {
		return null;
	}

}
