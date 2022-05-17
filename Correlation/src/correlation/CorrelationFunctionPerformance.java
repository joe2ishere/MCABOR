package correlation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import correlation.Estimators.CorrelationEstimator;
import util.Averager;
import util.getDatabaseConnection;

public class CorrelationFunctionPerformance {

	public static void main(String[] args) throws Exception {

		String withDebug[] = { ""/* , "_debug" */ };

		for (String d : withDebug) {
			System.out.println(d);
			TreeMap<String, ArrayList<Averager>> functionDayAverager = getFunctionDayAverage(d);
			DecimalFormat df = new DecimalFormat("#.##");

			for (String function : functionDayAverager.keySet()) {
				ArrayList<Averager> averages = functionDayAverager.get(function);
				System.out.print(function + ";");
				for (int i = 1; i < averages.size(); i++) {
					Averager average = averages.get(i);
					if (average.getCount() > 1)
						System.out.print(df.format(average.get()) /* + ";" + average.getCount() */);
					System.out.print(";");
				}
				System.out.println();

			}
		}

	}

	static String filename = "c:/users/joe/correlationOutput/correlationFunctionPerformance.csv";

	public static TreeMap<String, ArrayList<Averager>> loadFromFile() throws Exception {
		File f = new File(filename);

		String in;
		TreeMap<String, ArrayList<Averager>> functionDayAverager = new TreeMap<>();
		if (f.exists() == false)
			return functionDayAverager;
		BufferedReader br = new BufferedReader(new FileReader(filename));
		while ((in = br.readLine()) != null) {
			String ins[] = in.split(";");
			ArrayList<Averager> newList = new ArrayList<>();
			functionDayAverager.put(ins[0], newList);
			for (int i = 1; i < ins.length; i += 2) {
				Averager newAvg = new Averager();
				newList.add(newAvg);
				if (Double.parseDouble(ins[i + 1]) == 0)
					continue;
				newAvg.add(Double.parseDouble(ins[i]) / Double.parseDouble(ins[i + 1]));
			}
		}

		return functionDayAverager;
	}

	public static TreeMap<String, ArrayList<Averager>> getFunctionDayAverage(String d) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		PreparedStatement selectLastDate = conn
				.prepareStatement("select distinct mktDate from correlationfunctionresults" + d + " "
						+ " order by mktDate desc limit 30");
		ResultSet dateRS = selectLastDate.executeQuery();
		String latestDate = "";
		while (dateRS.next()) {
			latestDate = dateRS.getString(1);
		}

		PreparedStatement selectSymbols = conn.prepareStatement(
				"select distinct  symbol  from correlationfunctionresults" + d + "  where mktDate >= ? and "
						+ " symbol in (select distinct symbol from tsf_correlation) order by symbol");
		selectSymbols.setString(1, latestDate);
		ResultSet rsSymbols = selectSymbols.executeQuery();
		ArrayList<String> symbols = new ArrayList<>();
		while (rsSymbols.next()) {
			symbols.add(rsSymbols.getString(1));
		}
		rsSymbols.close();

		PreparedStatement selectFunctions = conn
				.prepareStatement("select distinct  function  from correlationfunctionresults" + d
						+ "  where mktDate >=" + " ? order by function");
		selectFunctions.setString(1, latestDate);
		ResultSet rsFunctions = selectFunctions.executeQuery();
		ArrayList<String> functions = new ArrayList<>();

		TreeMap<String, ArrayList<Averager>> functionDayAverager = new TreeMap<>();
		while (rsFunctions.next()) {
//			if (rsFunctions.getString(1).startsWith("smi"))
//				continue;
			functions.add(rsFunctions.getString(1));
			ArrayList<Averager> averages = new ArrayList<>();
			for (int i = 0; i <= 30; i++)
				averages.add(new Averager());
			functionDayAverager.put(rsFunctions.getString(1), averages);
		}
		rsFunctions.close();

		PreparedStatement selectMktDates = conn
				.prepareStatement("select distinct  mktDate, daysOut  from correlationfunctionresults" + d
						+ " where mktDate >= ? order by mktDate, daysOut asc");
		selectMktDates.setString(1, latestDate);
		ResultSet rsMktDates = selectMktDates.executeQuery();

		rsDates: while (rsMktDates.next()) {

			String mktDate = rsMktDates.getString(1);
//			if (mktDate.compareTo("2010-10") < 0)
//				continue;
			int daysOut = rsMktDates.getInt(2);
			boolean reported = false;

			for (String sym : symbols) {
				GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

				int dtPosition = gsd.dateIndex.get(mktDate);

				if (dtPosition + daysOut >= gsd.inDate.length)
					continue rsDates;

				if (reported == false) {
//					System.out.print(mktDate + ";" + daysOut);
//					System.out.println(gsd.inDate[dtlen + daysOut]);
//					System.out.println(gsd.inDate[dtlen]);
				}
				reported = true;

//				System.out.println(
//						"compare " + gsd.inDate[dtPosition + daysOut] + "to" + gsd.inDate[dtPosition] + " " + daysOut);
				boolean buy = gsd.inClose[dtPosition + daysOut] > gsd.inClose[dtPosition];
				for (String function : functions) {
					String func = function;
					if (func.endsWith("2"))
						func = func.substring(0, func.length() - 1);

					ArrayList<Averager> averages = functionDayAverager.get(func);
					PreparedStatement getResults = conn.prepareStatement("select guess from correlationfunctionresults"
							+ d + " where symbol = '" + sym + "' and " + " function = '" + function
							+ "' and mktDate = '" + mktDate + "' and daysOut = " + daysOut);

					ResultSet rsResults = getResults.executeQuery();
					if (rsResults.next() == false) {
						continue;
					}
					double guess = rsResults.getDouble(1);
					if (guess >= CorrelationEstimator.buyIndicatorLimit) {
//						if (function.compareTo("Bad") == 0)
//							System.out.println("at bad guess");
						if (buy)
							averages.get(daysOut).add(1);
						else
							averages.get(daysOut).add(0);
					} else if (guess <= CorrelationEstimator.sellIndicatorLimit) {
//						if (function.compareTo("Bad") == 0)
//							System.out.println("at bad guess");
						if (!buy)
							averages.get(daysOut).add(1);
						else
							averages.get(daysOut).add(0);
					} else {

					}

					rsResults.close();
				}

			}
		}
		rsMktDates.close();

		/*
		 * PrintWriter pw = new PrintWriter(filename); for (String key :
		 * functionDayAverager.keySet()) { pw.print(key); for (Averager avg :
		 * functionDayAverager.get(key)) { pw.print(";" + avg.getSum() + ";" +
		 * avg.getCount()); } pw.println(); } pw.flush(); pw.close();
		 */

		return functionDayAverager;

	}

}
