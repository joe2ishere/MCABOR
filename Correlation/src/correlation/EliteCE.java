package correlation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TreeMap;
import java.util.logging.LogManager;

import org.apache.commons.mail.HtmlEmail;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import bands.DeltaBands;
import util.Averager;
import util.getDatabaseConnection;
import weka.classifiers.Classifier;
import weka.core.Instances;

public class EliteCE extends CorrelationEstimator {

	public EliteCE(Connection conn) {
		super(conn);

	}

	public static void main(String[] args) throws Exception {

		LogManager.getLogManager().reset();
		ArrayList<CorrelationEstimator> estimators = new ArrayList<>();
		Connection conn = getDatabaseConnection.makeConnection();
		EliteCE ece = new EliteCE(conn);
		CorrelationEstimator.functionDayAverager = CorrelationFunctionPerformance.loadFromFile();
		ece.setDoElite();
		final PreparedStatement stGetDate = conn
				.prepareStatement("select distinct txn_date from etfprices order by txn_date desc limit 2");
		ResultSet rsDate = stGetDate.executeQuery();
		rsDate.next();
		currentMktDate = rsDate.getString(1);
		PrintWriter cePW = new PrintWriter("c:/users/joe/correlationOutput/cwToday" + currentMktDate + ".txt");
		cePW.println(currentMktDate);
		ArrayList<String> dtHeadings = dateList(currentMktDate, 5);
		rsDate.next();

		// estimators.add(new BBCorrelationEstimator(conn));
		estimators.add(new DMICorrelationEstimator(conn));
		estimators.add(new MACDCorrelationEstimator(conn));
		// estimators.add(new SMICorrelationEstimator(conn));
		estimators.add(new TSFCorrelationEstimator(conn));

		PreparedStatement selectSymbols = conn
				.prepareStatement("select distinct  symbol  from macd_correlation  order by symbol");
		ResultSet rsSymbols = selectSymbols.executeQuery();
		while (rsSymbols.next()) {
			String sym = rsSymbols.getString(1);
			GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
			gsds.put(sym, gsd);
			double[] symavg = new double[31];
			symbolDailyAverages.put(sym, symavg);
		}

		ArrayList<String> eliteList = new ArrayList<>();

		eliteList.add("qqq");
		eliteList.add("tqqq");
		eliteList.add("sqqq");
		;

		StringBuffer htmlMsg = new StringBuffer("<strong> <style> \n" + " table, th, td { border: 1px solid black; }"
				+ "</style>\n<table><tr><th style='text-align: center;'>Symbol<th style='text-align: center;'>"
				+ dtHeadings.get(0) + "<th style='text-align: center;'>" + dtHeadings.get(1)
				+ "<th style='text-align: center;'>" + dtHeadings.get(2) + "<th style='text-align: center;'>"
				+ dtHeadings.get(3) + "<th style='text-align: center;'>" + dtHeadings.get(4) + "</tr>\n");

		TreeMap<String, Double> theBadness = new TreeMap<>();

		for (String sym : eliteList) {
			System.out.print(sym);
			cePW.print(sym);
			htmlMsg.append("<tr><td style='text-align: center;'>" + sym + "</td>\n");
			Averager avverage = new Averager();
			TreeMap<Integer, Averager> avgForDaysOut = new TreeMap<>();
			for (int daysOut = 1; daysOut <= 5; daysOut++) {

				DeltaBands priceBands = new DeltaBands(gsds.get(sym).inClose, daysOut);

				avgForDaysOut.put(daysOut, new Averager());

				ArrayList<Thread> threads = new ArrayList<>();
				for (CorrelationEstimator ce : estimators) {

					ce.setWork(sym, daysOut, avverage, priceBands, avgForDaysOut, theBadness);
					Thread th = new Thread(ce);
					threads.add(th);
					th.start();
				}

				for (Thread th : threads) {
					th.join();
				}
			}
			for (Integer key : avgForDaysOut.keySet()) {
				double value = avgForDaysOut.get(key).get();
				System.out.print(";" + df.format(value));
				cePW.print(";" + value);
				htmlMsg.append(
						"<td align='right' bgcolor='" + getColorCode(value) + "'>" + df.format(value) + "</td>\n");
			}
			System.out.println();
			cePW.println();
			cePW.flush();
			htmlMsg.append("</tr>\n");
		}
		cePW.close();
		htmlMsg.append("</table></strong>");
		HtmlEmail eml = new HtmlEmail();
		eml.addTo("j.mcverry@americancoders.com");
		eml.setSubject("Elite CE Report for " + currentMktDate);
		eml.setFrom("support@mcverryreport.com");
		eml.setHtmlMsg(htmlMsg.toString());
		eml.setHostName("mcverryreport.com");
		eml.setSmtpPort(465);
		eml.setSSL(true);
		eml.setAuthentication("subscriptions@mcverryreport.com", "n4rlyW00D$");
		eml.send();
	}

	private static String getColorCode(double value) {
		Double dv = value;
		if (value <= 4.5) {
			dv = 255 * (value / 4.5);
			int hx = dv.intValue();
			return String.format("#FF%02X%02X", hx, hx);
		} else {
			dv = 255 * ((9 - value) / 4.5);
			int hx = dv.intValue();
			return String.format("#%02XFF%02X", hx, hx);

		}
	}

	@Override
	public void buildHeaders(PrintWriter pw) throws SQLException, Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void printAttributeData(int iday, int daysOutCalc, PrintWriter pw,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] inClose, DeltaBands priceBands) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void getAttributeText(PrintWriter pw, Object myParm, int functionDaysDiff2, int start, int doubleBack) {
		// TODO Auto-generated method stub

	}

	static ArrayList<String> dateList(String startAfter, int howMany) {

		Calendar clg = Calendar.getInstance();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat dfs = new SimpleDateFormat("E=d");
		ArrayList<String> retDates = new ArrayList<>();
		String dtfmt = df.format(clg.getTime());
		while (dtfmt.compareTo(startAfter) <= 0) {
			clg.add(Calendar.DAY_OF_MONTH, +1);
			dtfmt = df.format(clg.getTime());
		}

		ArrayList<String> holiDates = new ArrayList<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader("holidays.txt"));
			String holidate = "";
			while ((holidate = br.readLine()) != null) {
				holidate = holidate.trim();
				if (holidate.startsWith("#")) {
					continue;
				}
				holiDates.add(holidate);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		while (retDates.size() < howMany) {
			if (clg.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) {
				clg.add(Calendar.DAY_OF_MONTH, +1);
				continue;
			}
			if (clg.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
				clg.add(Calendar.DAY_OF_MONTH, +1);
				continue;
			}
			dtfmt = df.format(clg.getTime());
			String svDt = dfs.format(clg.getTime());
			clg.add(Calendar.DAY_OF_MONTH, +1);

			if (holiDates.contains(dtfmt))
				continue;

			retDates.add(svDt.replace("=", "<br>"));

		}

		return retDates;

	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

	@Override
	public double drun(Instances instances) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
