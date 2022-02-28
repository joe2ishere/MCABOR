package correlation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import utils.TopAndBottomList;

public class CorrelationUpdate {

	ArrayList<String> updateSymbol = null; // set to null when doing all closing symbols
	{

	}

	public ArrayList<String> getUpdateSymbolList() {
//		updateSymbol = new ArrayList<>();
//		updateSymbol.add("qyld");
		return updateSymbol;
	}

	ArrayList<String> doneList = new ArrayList<String>();
	TreeMap<String, Integer> symList = new TreeMap<>();
	TreeMap<String, GetETFDataUsingSQL> gsds = new TreeMap<>();
	double bestCorrelation = 0;
	TreeMap<String, TopAndBottomList> tops = new TreeMap<>();

	int volumeLimit = 10000;
	int entryLimit = 1500;

	public CorrelationUpdate(Connection conn) throws Exception {
		System.out.println(updateSymbol == null ? "Updating all symbols" : "partial update");
		buildLists(conn);
	}

	public ArrayList<String> tooHigh = new ArrayList<String>();

	public void buildLists(Connection conn) throws Exception {

		PreparedStatement psLastDate = conn
				.prepareStatement("select distinct txn_date from etfprices order by txn_date desc limit 1");
		ResultSet rsLastDate = psLastDate.executeQuery();
		if (rsLastDate.next() == false) {
			System.out.println("end of date error");
			System.exit(0);
		}

		String latestDate = rsLastDate.getString(1);

		PreparedStatement psStdTooHigh = conn.prepareStatement("SELECT symbol FROM stdofclose WHERE s > a and a > 25");
		ResultSet rsTooHigh = psStdTooHigh.executeQuery();
		while (rsTooHigh.next()) {
			tooHigh.add(rsTooHigh.getString(1).toLowerCase());
		}

		PreparedStatement ps = conn.prepareStatement(
				"select symbol, AVG(volume) AS vavg, count(*) as txncnt  from etfprices  GROUP BY symbol ORDER BY symbol");
		ResultSet rs = ps.executeQuery();

		while (rs.next()) {

			String sym = rs.getString(1).toLowerCase();
//			if (tooHigh.contains(sym)) {
//				System.out.println("skipping " + sym + " with std deviation price too high");
//				continue;
//			}

			double vol = rs.getDouble(2);

			if (vol < volumeLimit) {
				System.out.println("skipping " + sym + " with vol of " + vol);
				continue;
			}

			int cnt = rs.getInt(3);
			if (cnt < entryLimit) {
				System.out.println("skipping " + sym + " with cnt of " + cnt);
				continue;

			}

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(sym);
			if (pgsd.inDate[pgsd.inDate.length - 1].compareTo(latestDate) != 0) {
				System.out.println("skipping " + sym + " with end date of  " + pgsd.inDate[pgsd.inDate.length - 1]);
				continue;

			}

			gsds.put(sym, pgsd);
			symList.put(sym, cnt);

		}

	}

}
