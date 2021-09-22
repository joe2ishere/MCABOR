package correlation;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import bands.DeltaBands;
import util.getDatabaseConnection;

public class MakeARFF {

	public static void main(String args[]) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();

		PreparedStatement selectSymbols = conn
				.prepareStatement("select distinct  symbol  from TSF_correlation order by symbol");
		ResultSet rsSymbols = selectSymbols.executeQuery();

		while (rsSymbols.next()) {
			String sym = rsSymbols.getString(1);
			CorrelationEstimator.gsds.put(sym, GetETFDataUsingSQL.getInstance(sym));
		}
		String sym = "qqq";
		int daysOut = 15;
		CorrelationEstimator.currentMktDate = CorrelationEstimator.gsds
				.get(sym).inDate[CorrelationEstimator.gsds.get(sym).inDate.length - 1];
		CorrelationEstimator ce = new TSFCorrelationEstimator(conn);
		DeltaBands pb = new DeltaBands(CorrelationEstimator.gsds.get(sym).inClose, daysOut);
		TreeMap<String, Double> theBadness = new TreeMap<>();
		ce.setWork(sym, daysOut, pb, null, theBadness);
		String instances = ce.buildInstances();
		FileWriter fw = new FileWriter("c:/users/joe/correlationARFF/" + ce.function + sym + daysOut + ".arff");
		fw.write(instances.toCharArray(), 0, instances.toCharArray().length);
		fw.flush();
		fw.close();

	}
}
