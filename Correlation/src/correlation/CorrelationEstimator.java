package correlation;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.HtmlEmail;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.multipdf.PDFMergerUtility;

import com.americancoders.dataGetAndSet.GSDException;
import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import util.Averager;
import util.DateLine;
import util.getDatabaseConnection;
import utils.TopAndBottomList;
import weka.attributeSelection.ASEvaluation;
import weka.attributeSelection.ASSearch;
import weka.attributeSelection.AttributeSelection;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public abstract class CorrelationEstimator implements Runnable {
	static DecimalFormat df = new DecimalFormat("#.00");
	static DecimalFormat df4 = new DecimalFormat("#.####");

	static ArrayList<String> B5 = new ArrayList<>();
	static ArrayList<String> S5 = new ArrayList<>();
	static ArrayList<String> B10 = new ArrayList<>();
	static ArrayList<String> S10 = new ArrayList<>();

	static TreeMap<String, GetETFDataUsingSQL> gsds = new TreeMap<>();
	static ArrayList<String> primaryETFs = new ArrayList<String>();

	TreeMap<Integer, Averager> avgForDaysOut;

	static TreeMap<Integer, Favored> dailyHighlyFavored = new TreeMap<>();
	static TreeMap<Integer, Favored> dailyLeastFavored = new TreeMap<>();
	static ArrayList<String> doneList;

	static String pdfStartRowWithThinBorder = "<fo:table-row border='0.07px solid lightgrey'>";
	static String pdfStartRowWithBorder = "<fo:table-row border='.25px solid black'>";

	static String pdfSymbolCell = "<fo:table-cell ><fo:block>%s</fo:block></fo:table-cell>\n";
	static String pdfEndRow = "</fo:table-row>";
	static String pdfDataCell = "<fo:table-cell><fo:block background-color='%c'>%s</fo:block></fo:table-cell>\n";

	static double buyIndicatorLimit = 7.25;
	static double sellIndicatorLimit = 1.75;

	static File resultsForDBFile;
	static PrintWriter resultsForDBPrintWriter;

	static String updateOldFileDate = null;

	static boolean debugging = true;

	static boolean debuggingFast = false;

	static TreeMap<String, double[]> symbolDailyAverages = new TreeMap<>();

	static TreeMap<String, ArrayList<Averager>> functionDayAverager;

	Averager bestHigh = new Averager();
	Averager worstLow = new Averager();

	static boolean doingElite = false;

	static String csvFileType;

	String processDate;

	AttributeMakerFromSQL makeSQL;

	public void setDoElite() {
		doingElite = true;
	}

	public static void loadGSDSTable(Connection conn) throws Exception {
		PreparedStatement selectSymbols = conn.prepareStatement("SELECT DISTINCT symbol FROM prices WHERE symbol IN "
				+ "(SELECT DISTINCT symbol FROM dmi_correlation) OR "
				+ "symbol IN (SELECT DISTINCT functionsymbol  FROM dmi_correlation) OR "
				+ "symbol IN (SELECT DISTINCT symbol FROM macd_correlation) OR "
				+ "symbol IN (SELECT DISTINCT functionsymbol  FROM macd_correlation) OR "
				+ "symbol IN (SELECT DISTINCT symbol FROM sm_correlation) OR "
				+ "symbol IN (SELECT DISTINCT functionsymbol  FROM sm_correlation) OR "
				+ "symbol IN (SELECT DISTINCT symbol FROM tsf_correlation) OR "
				+ "symbol IN (SELECT DISTINCT functionsymbol  FROM tsf_correlation)  " + "order by symbol");
		ResultSet rsSymbols = selectSymbols.executeQuery();
		while (rsSymbols.next()) {
			String sym = rsSymbols.getString(1);
			GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
			gsds.put(sym, gsd);
		}
		selectSymbols = conn.prepareStatement("SELECT DISTINCT symbol FROM dmi_correlation WHERE symbol IN "
				+ "(SELECT DISTINCT symbol FROM macd_correlation) and "
				+ "symbol IN (SELECT DISTINCT symbol FROM sm_correlation) and "
				+ "symbol IN (SELECT DISTINCT symbol FROM tsf_correlation) ");
		rsSymbols = selectSymbols.executeQuery();
		while (rsSymbols.next()) {
			String sym = rsSymbols.getString(1);
			primaryETFs.add(sym);
		}
		Collections.sort(primaryETFs);
	}

	public static void main(String args[]) throws Exception {
		if (args.length > 0) {
			if (args[0].startsWith("-debug")) {
				debugging = true;
				csvFileType = "Debug.csv";
			} else if (args[0].startsWith("-auto")) {
				debugging = false;
				csvFileType = ".csv";
			} else {
				System.out.println("you need to pass -debug or -auto  ");
				return;
			}
		} else {
			System.out.println("you need to pass -debug or -auto  ");
			InputStreamReader isr = new InputStreamReader(System.in);
			BufferedReader br = new BufferedReader(isr);
			while (true) {
				System.out.println("(D)ebug or (A)uto  ");
				String debugAuto = br.readLine() + " ";
				if (debugAuto.startsWith("D")) {
					debugging = true;
					System.out.println("(F)ast or (S)low");
					String debugFastOrSlow = br.readLine() + " ";
					if (debugFastOrSlow.startsWith("F")) {
						debuggingFast = true;
						break;
					}
					csvFileType = "Debug.csv";
					break;
				}

				if (debugAuto.startsWith("A")) {
					debugging = false;
					csvFileType = ".csv";
					break;
				}

				java.awt.Toolkit.getDefaultToolkit().beep();
			}

		}

		LogManager.getLogManager().reset();

		if (debugging)
			functionDayAverager = CorrelationFunctionPerformance.loadFromFile();
		else
			functionDayAverager = CorrelationFunctionPerformance.getFunctionDayAverage("");

		Connection conn = getDatabaseConnection.makeConnection();
		final PreparedStatement stGetDate = conn
				.prepareStatement("select distinct txn_date from etfprices order by txn_date desc limit 2");
		ResultSet rsDate = stGetDate.executeQuery();
		rsDate.next();

		currentMktDate = rsDate.getString(1);

		rsDate.next();
		String previousMarketDate = rsDate.getString(1);

		Connection connRemote = null;
		try {
			connRemote = getDatabaseConnection.makeMcVerryReportConnection();
			doneList = getWhatWasPrintedYesterday(connRemote, previousMarketDate);
			connRemote.close();
		} catch (SQLException se) {
			System.out.println(se.getErrorCode());
			System.out.println(se.getMessage());
			System.out.println(se.getSQLState());
			return;

		}
		resultsForDBFile = new File(
				"c:/users/joe/correlationOutput/resultsForFunctionTest_" + currentMktDate + csvFileType);

		resultsForDBPrintWriter = new PrintWriter(resultsForDBFile);

		PrintWriter averageOutPW = new PrintWriter("c:/users/joe/correlationOutput/" + currentMktDate + csvFileType);

		File rawDataCSVFile = new File("c:/users/joe/correlationOutput/rawData" + currentMktDate + csvFileType);

		PrintWriter rawDataCSVPW = new PrintWriter(rawDataCSVFile);

		File estimatedPriceCSVFile = new File(
				"c:/users/joe/correlationOutput/estimatedPrices" + currentMktDate + csvFileType);

		PrintWriter estimatedPriceCSVPW = new PrintWriter(estimatedPriceCSVFile);

		PreparedStatement psInsertToSQL = conn
				.prepareStatement("insert into correlation30days (mktDate, symbol, guesses) values('" + currentMktDate
						+ "',?,?)" + "on duplicate key update guesses=?");

		// PrintStream averageOutPW = System.out;

		StringWriter rptSw = new StringWriter();
		PrintWriter rptOut = new PrintWriter(rptSw);

		ArrayList<String> datesApart = null;
		TreeMap<String, String> day5 = new TreeMap<>();
		TreeMap<String, String> day10 = new TreeMap<>();
		TreeMap<String, String> day15 = new TreeMap<>();
		loadGSDSTable(conn);
		for (String sym : gsds.keySet()) {
			GetETFDataUsingSQL gsd = gsds.get(sym);
			double[] symavg = new double[31];
			symbolDailyAverages.put(sym, symavg);
			if (datesApart == null) {
				datesApart = new ArrayList<>();
				for (int i = 5; i <= 30; i += 5) {
					datesApart.add(gsd.inDate[gsd.inDate.length - (i + 1)]);
					if (i == 25)
						updateOldFileDate = gsd.inDate[gsd.inDate.length - (i + 1)];
				}

			}

		}

		// System.exit(0);

		TreeMap<String, TreeMap<Integer, Averager>> catAverages = new TreeMap<>();
		TreeMap<String, StringBuffer> catETFList = new TreeMap<>();
		TreeMap<String, String> symCats = new TreeMap<>();

		PreparedStatement selectCategory = conn
				.prepareStatement("select symbol, category from etfcategory order by symbol");

		ResultSet rsCategory = selectCategory.executeQuery();
		while (rsCategory.next()) {
			String sym = rsCategory.getString(1).trim();
			String cat = rsCategory.getString(2).trim();
			if (cat.length() == 0)
				continue;
			TreeMap<Integer, Averager> catAvg = catAverages.get(cat);
			if (catAvg == null) {
				catAvg = new TreeMap<Integer, Averager>();
				catAverages.put(cat, catAvg);
				catETFList.put(cat, new StringBuffer(1000));
			}
			symCats.put(sym, cat);
			StringBuffer catList = catETFList.get(cat);
			catList.append(sym + " ");
		}

		final PreparedStatement selectDays = conn
				.prepareStatement("select distinct toCloseDays from tsf_correlation" + "  order by toCloseDays");
		ResultSet rsSelectDays = selectDays.executeQuery();
		ArrayList<Integer> daysOutList = new ArrayList<Integer>();
		System.out.print("Daily symbol");
		averageOutPW.print("Daily");
		rawDataCSVPW.print("Symbol");
		estimatedPriceCSVPW.print("Symbol;Close");

		String hdgDate = DateLine.dateLine(currentMktDate, 30);
		averageOutPW.print(hdgDate);
		rawDataCSVPW.print(hdgDate);
		estimatedPriceCSVPW.print(hdgDate);
		System.out.print(hdgDate);

		while (rsSelectDays.next()) {
			int day = rsSelectDays.getInt(1);
			daysOutList.add(day);
			dailyHighlyFavored.put(day, new Favored("", -1.));
			dailyLeastFavored.put(day, new Favored("", 99.));

		}
		System.out.println(";Avg. 1-5; Avg. 6-10; Avg. 11-15;Avg. 16-20; Avg. 21-25; Avg. 26-30;Avg.");
		averageOutPW.println(";Avg. 1-5; Avg. 6-10; Avg. 11-15;Avg. 16-20; Avg. 21-25; Avg. 26-30;Avg.");
		rawDataCSVPW.println();
		estimatedPriceCSVPW.println();

		ArrayList<CorrelationEstimator> estimators = new ArrayList<>();

		// TODO set estimators
		estimators.add(new SMICorrelationEstimator(conn));
		estimators.add(new ATRandMACDCorrelationEstimator(conn));
		estimators.add(new MACDandMACorrelationEstimator(conn));
		estimators.add(new ATRCorrelationEstimator(conn));
		estimators.add(new DMICorrelationEstimator(conn));
		estimators.add(new MACDCorrelationEstimator(conn));
		estimators.add(new MAAveragerCorrelationEstimator(conn));
		estimators.add(new MALinesCorrelationEstimator(conn));
		estimators.add(new TSFCorrelationEstimator(conn));
		estimators.add(new MACorrelationEstimator(conn));
		// TODO set estimators

		StringBuffer etfExpectations = new StringBuffer(1000);
		StringBuffer etfExpectationsAbridged = new StringBuffer(1000);

		ArrayList<String> categoryDoneforDebugging = new ArrayList<>();

		for (String sym : primaryETFs) {

			if (debugging) {

				if (debuggingFast) {

					PreparedStatement selectCategoryBySybmolforDebug = conn
							.prepareStatement("select category from etfcategory where symbol = ?");
					selectCategoryBySybmolforDebug.setString(1, sym);
					ResultSet scbsfd = selectCategoryBySybmolforDebug.executeQuery();
					if (scbsfd.next()) {
						String cat = scbsfd.getString("category").trim();
						if (cat.length() < 1)
							continue;
						if (categoryDoneforDebugging.contains(cat))
							continue;
						categoryDoneforDebugging.add(cat);
					}

//					if (sym.startsWith("kre") == false)
//						continue;
				}
			}
			System.out.print(sym);
			averageOutPW.print(sym);
			rawDataCSVPW.print(sym);
			estimatedPriceCSVPW.print(sym);

			psInsertToSQL.setString(1, sym);

			etfExpectations.append(pdfStartRowWithThinBorder);
			etfExpectations.append(pdfSymbolCell.replace("%s", sym));

			StringBuffer sqlBuffer = new StringBuffer(1000);

			TreeMap<Integer, Averager> avgForDaysOut = new TreeMap<>();

			TreeMap<Integer, Averager> catAvg = null;
			if (symCats.containsKey(sym))
				catAvg = catAverages.get(symCats.get(sym));

			TreeMap<String, Double> theBadness = new TreeMap<>();

			boolean[] biggerHalfFirst = { true, false };

			for (int daysOut : daysOutList) {
				avgForDaysOut.put(daysOut, new Averager());
				Semaphore threadSemaphore = new Semaphore(4);
				DeltaBands priceBands = new DeltaBands(gsds.get(sym).inClose, daysOut);
				for (boolean whichHalf : biggerHalfFirst) {
					{
						ArrayList<Thread> threads = new ArrayList<>();
						for (CorrelationEstimator ce : estimators) {
							ce.setWork(sym, daysOut, priceBands, avgForDaysOut, theBadness, whichHalf, threadSemaphore);
							threadSemaphore.acquire();
							Thread th = new Thread(ce);
							threads.add(th);
							th.start();
						}
						for (Thread th : threads) {
							th.join();
						}
					}

				}

			}

			for (int daysOut : daysOutList) {
				try {
					String badnessFileName = "c:/users/joe/correlationARFF/bad/" + sym + "_" + daysOut
							+ "_allbad_correlation.arff";
					StringBuffer sb = new StringBuffer(1000);
					String in = "";
					BufferedReader brBad = new BufferedReader(new FileReader(badnessFileName));
					while ((in = brBad.readLine()) != null) {
						sb.append(in + "\n");
					}
					brBad.close();
					sb.append(theBadness.get("dmi2;" + daysOut) + ",");
					sb.append(theBadness.get("macd2;" + daysOut) + ",");
					sb.append(theBadness.get("maAvg2;" + daysOut) + ",");
					sb.append(theBadness.get("mali2;" + daysOut) + ",");
					sb.append(theBadness.get("smi2;" + daysOut) + ",");
					sb.append(theBadness.get("tsf2;" + daysOut) + ",");
					sb.append("?");
					Instances instances = new Instances(new StringReader(sb.toString()));
					int classPos = instances.numAttributes() - 1;
					instances.setClassIndex(classPos);

					AttributeSelection as = new AttributeSelection();
					ASSearch asSearch = ASSearch.forName("weka.attributeSelection.GreedyStepwise",
							new String[] { "-B", "-R" });
					as.setSearch(asSearch);
					ASEvaluation asEval = ASEvaluation.forName("weka.attributeSelection.CfsSubsetEval",
							new String[] { "-M" });
					as.setEvaluator(asEval);
					as.SelectAttributes(instances);
					instances = as.reduceDimensionality(instances);
					Classifier classifier = AbstractClassifier.forName("weka.classifiers.lazy.LWL",
							new String[] { "-A", "weka.core.neighboursearch.LinearNNSearch", "-W",
									"weka.classifiers.functions.LinearRegression", "--", "-S", "1", "-C", "-R",
									"0.0011526567509185315" });
					classifier.buildClassifier(instances);

					double got = classifier.classifyInstance(instances.get(instances.size() - 1));
					if (got > 9.0)
						got = 9.0;
					if (got < 0.0)
						got = 0.0;
					setResultsForDBForBadNess(got, sym, daysOut);
					double dd = daysOut;
					avgForDaysOut.get(daysOut).add(got, dd / 5);
				} catch (Exception e) {
					System.out.println(e.getLocalizedMessage());
					System.out.println("continuing");
				}
//				// daysOut / 5 is a weight that shows that
//				// the lower dates are less effective and the higher dates are much more
//				// effective

			}

			double savg[] = new double[31];
			/* if (one80Mode == false) */ {
				estimatedPriceCSVPW.print(";" + df.format(gsds.get(sym).inClose[gsds.get(sym).inClose.length - 1]));
			}
			for (Integer key : avgForDaysOut.keySet()) {
				double theAvg = avgForDaysOut.get(key).get();
				System.out.print(";" + df.format(theAvg));
				averageOutPW.print(";" + df.format(theAvg));
				rawDataCSVPW.print(";" + df.format(theAvg));
				DeltaBands priceBands = new DeltaBands(gsds.get(sym).inClose, key);
				estimatedPriceCSVPW.print(";" + df.format(gsds.get(sym).inClose[gsds.get(sym).inClose.length - 1]
						* (1. + priceBands.getApproximiateValue(theAvg))));
				savg[key] = theAvg;
				etfExpectations.append(formatPDFCell(theAvg));
				sqlBuffer.append(df4.format(theAvg) + ";");
				if (catAvg != null) {
					Averager avg = catAvg.get(key);
					if (avg == null) {
						avg = new Averager();
						catAvg.put(key, avg);
					}
					avg.add(theAvg);
				}
				Favored fav = dailyHighlyFavored.get(key);
				if (fav.getValue() < theAvg)
					fav.setSymbolValue(sym, theAvg);
				else if (fav.getValue() == theAvg)
					fav.addSymbol(sym);
				fav = dailyLeastFavored.get(key);
				if (fav.getValue() > theAvg)
					fav.setSymbolValue(sym, theAvg);
				else if (fav.getValue() == theAvg)
					fav.addSymbol(sym);
			}

			symbolDailyAverages.put(sym, savg);

			StringBuffer webSiteReportSB = new StringBuffer("<tr><td>" + sym + "</td>");
			StringBuffer etfExpectationReportWaitForBuyOrSell = new StringBuffer(pdfStartRowWithThinBorder);
			etfExpectationReportWaitForBuyOrSell.append(pdfSymbolCell.replace("%s", sym));

			psInsertToSQL.setString(2, sqlBuffer.toString());
			psInsertToSQL.setString(3, sqlBuffer.toString());
			if (debugging) {
				System.out.println("debugging, so psInsertToSQL is not run.");
			} else {
				psInsertToSQL.execute();
			}

			double averages[] = new double[30];
			for (Integer daysOut : avgForDaysOut.keySet()) {
				averages[daysOut - 1] = avgForDaysOut.get(daysOut).get();
			}
			boolean buySellWrittenToWebPage = false;
			Averager avverager = new Averager();
			for (int daysOut = 4, tdc = 1; daysOut <= 29; daysOut += 5, tdc++) {

				Averager outAvg = new Averager();
				for (int daysBack = 0; daysBack <= 4; daysBack++) {
					outAvg.add(averages[daysOut - (4 - daysBack)], (daysBack + 1) * (daysBack + 1));
				}

				avverager.add(outAvg.get());
				System.out.print(";" + df.format(outAvg.get()));
				averageOutPW.print(";" + df.format(outAvg.get()));

				webSiteReportSB.append("td" + tdc);

				etfExpectations.append(formatPDFCell(outAvg.get()));

				if (outAvg.get() > buyIndicatorLimit) {

					webSiteReportSB.append("B");
					buySellWrittenToWebPage = true;
					String forAbridged = pdfDataCell.replace("%c", "green");
					etfExpectationReportWaitForBuyOrSell.append(forAbridged.replace("%s", "Bullish"));

					{
						if (daysOut == 4) {
							if (doneList.contains(sym) == false) {
								B5.add(sym);
								day5.put(sym, "Bullish");
							}
							doneList.add(sym);

						} else if (daysOut == 9 & B5.contains(sym) == false & S5.contains(sym) == false) {
							if (doneList.contains(sym) == false & B5.contains(sym) == false) {
								B10.add(sym);
								day10.put(sym, "Bullish");
							}
							doneList.add(sym);

						}
					}
				} else if (outAvg.get() < sellIndicatorLimit) {

					webSiteReportSB.append("S");
					buySellWrittenToWebPage = true;
					String forAbridged = pdfDataCell.replace("%c", "red");
					etfExpectationReportWaitForBuyOrSell.append(forAbridged.replace("%s", "Bearish"));

					{
						if (daysOut == 4) {
							if (doneList.contains(sym) == false) {
								S5.add(sym);
								day5.put(sym, "Bearish");
							}
							doneList.add(sym);

						} else if (daysOut == 9 & B5.contains(sym) == false & S5.contains(sym) == false) {
							if (doneList.contains(sym) == false & S5.contains(sym) == false) {
								S10.add(sym);
								day10.put(sym, "Bearish");
							}
							doneList.add(sym);

						}
					}

				} else {
					webSiteReportSB.append("-");
					String forAbridged = pdfDataCell.replace("%c", "white");
					etfExpectationReportWaitForBuyOrSell.append(forAbridged.replace("%s", ""));
				}
			}

			webSiteReportSB.append("</tr>");
			System.out.println(";" + df.format(avverager.get()));
			averageOutPW.println(";" + df.format(avverager.get()));
			rawDataCSVPW.println();
			estimatedPriceCSVPW.println();

			averageOutPW.flush();
			rawDataCSVPW.flush();
			estimatedPriceCSVPW.flush();

			if (buySellWrittenToWebPage == true) {
				rptOut.print(webSiteReportSB.toString());
				rptOut.flush();
				etfExpectationsAbridged.append(etfExpectationReportWaitForBuyOrSell.toString());
				etfExpectationsAbridged.append(pdfEndRow);
			}

			etfExpectations.append(formatPDFCell(avverager.get()));
			etfExpectations.append(pdfEndRow);
//			for (String key : threadRunAverages.keySet()) {
//				System.out.println(key + ";" + threadRunAverages.get(key).get());
//			}
//			System.exit(0);
		}

		// makeBestWorstDaily
		StringBuffer bestDailyTop = new StringBuffer(1000);
		StringBuffer bestDailyBottom = new StringBuffer(1000);
		StringBuffer worstDailyTop = new StringBuffer(1000);
		StringBuffer worstDailyBottom = new StringBuffer(1000);
		bestDailyTop.append(pdfStartRowWithThinBorder);
		bestDailyBottom.append(pdfStartRowWithThinBorder);
		worstDailyTop.append(pdfStartRowWithThinBorder);
		worstDailyBottom.append(pdfStartRowWithThinBorder);
		for (Integer key : dailyHighlyFavored.keySet()) {
			Favored fav = dailyHighlyFavored.get(key);
			bestDailyTop.append(pdfSymbolCell.replace("%s", fav.symbol));
			bestDailyBottom.append(formatPDFCell(fav.value));
			fav = dailyLeastFavored.get(key);
			worstDailyTop.append(pdfSymbolCell.replace("%s", fav.symbol));
			worstDailyBottom.append(formatPDFCell(fav.value));
		}
		bestDailyTop.append(pdfEndRow);
		bestDailyBottom.append(pdfEndRow);
		worstDailyTop.append(pdfEndRow);
		worstDailyBottom.append(pdfEndRow);
		resultsForDBPrintWriter.flush();
		resultsForDBPrintWriter.close();
		updateResultsTable(conn, resultsForDBFile);

		System.out.println();
		averageOutPW.flush();
		averageOutPW.close();
		if (debugging) {
			System.out.println("debugging, so insert into forecastReport  is not run.");
		} else {
			connRemote = getDatabaseConnection.makeMcVerryReportConnection();
			PreparedStatement insertIntoRemoteReport102030 = connRemote
					.prepareStatement("insert into forecastReport  (dateOfReport, rptAvailBy, currentReport,"
							+ " 5DayResult, 10DayResult, 15DayResult,20DayResult, 25DayResult, 30DayResult)"
							+ " values(?,?,?,'Not Available','Not Available','Not Available','Not Available','Not Available','Not Available' )"
							+ " on duplicate key update currentReport=?");
			insertIntoRemoteReport102030.setString(1, currentMktDate);
			insertIntoRemoteReport102030.setString(2, ""); // getNextReportAvailableBy());
			insertIntoRemoteReport102030.setString(3, rptSw.toString());
			insertIntoRemoteReport102030.setString(4, rptSw.toString());

			insertIntoRemoteReport102030.execute();
		}

		rptOut.flush();

		StringBuffer sbCategoryReport = new StringBuffer(1000);
		TreeMap<String, double[]> dCatAverages = new TreeMap<>();
		Core core = new Core();

		rawDataCSVPW.println();
		rawDataCSVPW.println();
		rawDataCSVPW.println();

		for (String key : catAverages.keySet()) {
			sbCategoryReport.append(pdfStartRowWithThinBorder);
			sbCategoryReport.append(pdfSymbolCell.replace("%s", key));
			rawDataCSVPW.print(key.replace("&amp;", "&") + ";");

			TreeMap<Integer, Averager> catAverage = catAverages.get(key);
			for (Integer dayout : catAverage.keySet()) {
				Averager avg = catAverage.get(dayout);
				sbCategoryReport.append(formatPDFCell(avg.get()));
				rawDataCSVPW.print(String.format("%1.1f", avg.get()) + ";");

			}
			sbCategoryReport.append(pdfEndRow);
			rawDataCSVPW.println();

		}

		rawDataCSVPW.flush();
		rawDataCSVPW.close();
		rawDataCSVPW.flush();
		rawDataCSVPW.close();

		TopAndBottomList catOverallAverages = new TopAndBottomList();
		for (String key : catAverages.keySet()) {
			TreeMap<Integer, Averager> catAverage = catAverages.get(key);
			Averager thisCatAverage = new Averager();
			for (int lookAheadAverage = 1; lookAheadAverage <= 30; lookAheadAverage++) {
				for (Integer dayout : catAverage.keySet()) {
					Averager avg = catAverage.get(dayout);
					thisCatAverage.add(avg.get());
				}
			}
			catOverallAverages.set(thisCatAverage.get(), key);
		}
		TopAndBottomList tabLookAhead = new TopAndBottomList();
		for (int lookAheadAverage = 4; lookAheadAverage <= 10; lookAheadAverage++) {
			if (lookAheadAverage == 5)
				continue;
			for (String key : catAverages.keySet()) {
				TreeMap<Integer, Averager> catAverage = catAverages.get(key);
				double tempCatAverage[] = new double[30];
				for (Integer dayout : catAverage.keySet()) {
					Averager avg = catAverage.get(dayout);
					tempCatAverage[dayout - 1] = avg.get();
				}
				double dCatAverage[] = new double[30];
				MInteger outBegIdx = new MInteger();
				MInteger outNBElement = new MInteger();
				core.ema(0, tempCatAverage.length - 1, tempCatAverage, lookAheadAverage, outBegIdx, outNBElement,
						dCatAverage);

				dCatAverages.put(key, dCatAverage);
			}

			for (String key : dCatAverages.keySet()) {
				double dCatAverage[] = dCatAverages.get(key);
				for (int i = 0; i <= 30 - lookAheadAverage; i++) {
					tabLookAhead.setTop(dCatAverage[i], key + "<" + (i + 1) + " to " + (i + lookAheadAverage));
					tabLookAhead.setBottom(dCatAverage[i], key + "<" + (i + 1) + " to " + (i + lookAheadAverage));
				}
			}
		}
		StringBuffer emailText = new StringBuffer(1000);

		if ((catOverallAverages.getTopValue(0) - 4.5) > (4.5 - catOverallAverages.getBottomValue(0))) {
			topReport(true, emailText, catOverallAverages, tabLookAhead, catETFList);
			bottomReport(false, emailText, catOverallAverages, tabLookAhead, catETFList);
		} else {
			bottomReport(true, emailText, catOverallAverages, tabLookAhead, catETFList);
			topReport(false, emailText, catOverallAverages, tabLookAhead, catETFList);
		}

		FileReader fr = new FileReader("xmlFilesForPDFReports/Report-FO.xml");
		BufferedReader br = new BufferedReader(fr);
		String in = "";
		StringBuffer fsb = new StringBuffer(1000);
		while ((in = br.readLine()) != null) {
			fsb.append(in + "\n");
		}
		br.close();

		String xmlText = fsb.toString().replace("<<<<MARKET DATE>>>>", currentMktDate);
		if (etfExpectationsAbridged.length() < 2) {
			etfExpectationsAbridged.append("<fo:table-row background-color='lightgrey' >");
			etfExpectationsAbridged.append("<fo:table-cell><fo:block>No Data To Report</fo:block></fo:table-cell>");
			etfExpectationsAbridged.append("</fo:table-row>");
		}
		xmlText = xmlText.replace("<<<<ABRIDGED REPORT>>>>", etfExpectationsAbridged.toString());
		xmlText = xmlText.replace("<<<<CELL DATA>>>>", etfExpectations.toString());

		xmlText = xmlText.replace("<<<<BEST DAILY ETFS>>>>", bestDailyTop.toString() + bestDailyBottom.toString());

		xmlText = xmlText.replace("<<<<WORST DAILY ETFS>>>>", worstDailyTop.toString() + worstDailyBottom.toString());

		xmlText = xmlText.replace("<<<<CATEGORY DATA>>>>", sbCategoryReport.toString());
		xmlText = xmlText.replace("<<<<COPYRIGHT YEAR>>>>", currentMktDate.substring(0, 4));

		connRemote = getDatabaseConnection.makeMcVerryReportConnection();
		String historyForPDF = doHistory(connRemote, datesApart, currentMktDate);
		xmlText = xmlText.replace("<<<<HISTORY PDF REPORT>>>>", historyForPDF);

		StringBuffer catETFsb = new StringBuffer(1000);
		for (String key : catETFList.keySet()) {
			catETFsb.append("<fo:table-row>");
			catETFsb.append(
					"<fo:table-cell  text-align='center' border='solid 0.05mm black'><fo:block font-size='11pt'>");
			catETFsb.append(key);
			catETFsb.append("</fo:block></fo:table-cell>");
			catETFsb.append(
					"<fo:table-cell  text-align='center' border='solid 0.05mm black'><fo:block font-size='12pt' text-align='left'>");
			catETFsb.append(catETFList.get(key).toString());
			catETFsb.append("</fo:block></fo:table-cell>");
			catETFsb.append("</fo:table-row>");
		}
		xmlText = xmlText.replace("<<<<CATEGORY LIST DATA>>>>", catETFsb.toString());
		PrintWriter pxml = new PrintWriter("temp.xml");
		pxml.print(xmlText);
		pxml.flush();
		pxml.close();

		String pdfReportName;
		if (debugging)
			pdfReportName = PDFReport.makeReport(new ByteArrayInputStream(xmlText.getBytes(StandardCharsets.UTF_8)),
					currentMktDate, "debug_");
		else
			pdfReportName = PDFReport.makeReport(new ByteArrayInputStream(xmlText.getBytes(StandardCharsets.UTF_8)),
					currentMktDate, "");
		String performanceReportName = PerformanceFromDBForPDF.makeReport(currentMktDate);

		if (debugging)
			;
		else
			try {

				FTPClient ftpc = new FTPClient();
				ftpc.connect("mcverryreport.com");

				ftpc.login("reportly@mcverryreport.com", "*gg77_3*g168");

				ftpc.setControlKeepAliveTimeout(300);
				ftpc.enterLocalPassiveMode();
				ftpc.setFileType(FTP.BINARY_FILE_TYPE);
				ftpc.setFileTransferMode(FTP.BINARY_FILE_TYPE);

				ftpc.storeFile("subscribe/reportPerformance.pdf", new FileInputStream(performanceReportName));
				File oldreportfile = new File(
						"c:/users/joe/correlationOutput/ETFExpectations_" + updateOldFileDate + ".pdf");
				if (oldreportfile.exists())
					ftpc.storeFile("subscribe/SampleReport.pdf", new FileInputStream(oldreportfile));
				else {
					System.out.println("Did not store " + oldreportfile.getName()
							+ " file not found; will try shorttermtraderreport");
					oldreportfile = new File(
							"c:/users/joe/correlationOutput/ShortTermTraderReport_" + updateOldFileDate + ".pdf");
					if (oldreportfile.exists())
						ftpc.storeFile("subscribe/SampleReport.pdf", new FileInputStream(oldreportfile));
					else {
						System.out.println(
								"Did not store " + oldreportfile.getName() + " as SampleReport.pdf - file not found.");
					}

				}

				ftpc.disconnect();

			} catch (Exception e) {
				e.printStackTrace();
			}

		PDFMergerUtility merger = new PDFMergerUtility();
		String pdfBothReportName = "c:/users/joe/correlationOutput/etfExpectations_" + currentMktDate + ".pdf";
		merger.setDestinationFileName(pdfBothReportName);
		merger.addSource(new File(pdfReportName));
		merger.addSource(new File(performanceReportName));

		merger.mergeDocuments(MemoryUsageSetting.setupMainMemoryOnly());
		boolean lastDayOfWeek = isTodayLastDayOfWeek();
		Logger logr = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
		String logFileName = String.format("c:/users/joe/correlationOutput/mailOut-log." + currentMktDate + ".txt");

		FileHandler handler = new FileHandler(logFileName, true);
		handler.setFormatter(new SimpleFormatter());
		logr.addHandler(handler);
		logr.setLevel(Level.ALL);

		if (debugging) {

			HtmlEmail eml = new HtmlEmail();
			eml.addTo("j.mcverry@americancoders.com");
//			eml.addTo("usacoder@icloud.com");
			eml.setSubject("Reports");
			eml.setFrom("support@mcverryreport.com");

			StringBuffer htmlMsg = new StringBuffer("<html><body>");
			htmlMsg.append("<h2>ETF Expectations for " + currentMktDate + "</h2>");

			htmlMsg.append("<h3>Here is a quick analysis of the sectors. </h3>");
			htmlMsg.append(emailText.toString());
			htmlMsg.append("</body></html>");
			eml.setHtmlMsg(htmlMsg.toString());

			EmailAttachment attachment = new EmailAttachment();
			attachment.setPath(pdfReportName);
			attachment.setDisposition(EmailAttachment.ATTACHMENT);
			eml.attach(attachment);
			EmailAttachment attachment2 = new EmailAttachment();
			attachment2.setPath(performanceReportName);
			attachment2.setDisposition(EmailAttachment.ATTACHMENT);
			eml.attach(attachment2);
			EmailAttachment attachment3 = new EmailAttachment();
			attachment3.setPath(rawDataCSVFile.getPath());
			attachment3.setDisposition(EmailAttachment.ATTACHMENT);
			eml.attach(attachment3);
			EmailAttachment attachment4 = new EmailAttachment();
			attachment4.setPath(estimatedPriceCSVFile.getPath());
			attachment4.setDisposition(EmailAttachment.ATTACHMENT);
			eml.attach(attachment4);
			eml.setHostName("mcverryreport.com");
			eml.setSmtpPort(465);
			eml.setSSL(true);
			eml.setAuthentication("subscriptions@mcverryreport.com", "n4rlyW00D$");
			String emlret = eml.send();
			logr.info("debugging on; sent to j.mcverry@americancoders.com:" + emlret);

		} else {

			connRemote = getDatabaseConnection.makeMcVerryReportConnection();
			PreparedStatement psmail = connRemote.prepareStatement(
					"select email, type, count, idx from subscribers where status = 'Active' and count > 0");
			PreparedStatement updatemail = connRemote
					.prepareStatement("update subscribers set count = count - 1, lastSent = ? where idx = ? ");
			ResultSet rsmail = psmail.executeQuery();

			while (rsmail.next()) {
				// MultiPartEmail eml = new MultiPartEmail();
				String emailid = rsmail.getString("email").trim();
				String type = rsmail.getString("type").trim();
				int cnt = rsmail.getInt("count") - 1;
				int idx = rsmail.getInt("idx");
				if (type.endsWith("End of Week") & lastDayOfWeek == false)
					continue;
				HtmlEmail eml = new HtmlEmail();
				System.out.println("report send to: " + emailid + " type: " + type + " count:" + cnt);
				eml.addTo(emailid);

				logr.info(emailid + ": setup ");
				eml.setSubject("ETF Expectations for " + currentMktDate);
				eml.setFrom("support@mcverryreport.com");
				StringBuffer htmlMsg = new StringBuffer("<html><body>");
				if (type.startsWith("Compli")) {

					htmlMsg.append(" This is your complimentary copy of ETF Expectations for " + currentMktDate
							+ ".<p>To begin your subscription use the following link "
							+ "<br> https://mcverryreport.com/subscriptionRequest.html </p> ");

				} else {
					htmlMsg.append("<h2>ETF Expectations for " + currentMktDate + "</h2>");
				}
//				if (rsmail.getString(1).contains("usacoder") | rsmail.getString(1).contains("j.mcverry")) {
//				htmlMsg.append(
//						"<h3>I apologize for the duplicate email.  The \"5 Day Period Best Buy/Sell Indications\""
//								+ " report had several missing elements.  I squashed the bug that "
//								+ "caused the error.  </h3></p>");

//				htmlMsg.append(
//						"<h3>I apologize for the duplicate email.  The \"5 Day Period Strongest Bearish and Bullish Indications\""
//								+ " report had several missing elements.  I squashed the bug that "
//								+ "caused the error. Attached is the corrected report. </h3></p>");

				if (emailText.length() < 10) {
					htmlMsg.append("<h5>Since there are no significant data factors, there will be no "
							+ "quick sector analysis this evening. </h5>");

				} else {
					htmlMsg.append("<h3>Here is a quick analysis of the sectors. </h3>");
					htmlMsg.append(emailText.toString());
				}
//				}

				/*
				 * if (type.endsWith("Daily"))
				 * htmlMsg.append("<h5>Included with this email is a CSV file. " +
				 * "The file contains estimated closing prices " +
				 * "for the next 30 days for each of the ETFs I track. </h5><p/>");
				 */

				htmlMsg.append("<p>Your subscription count is now at " + cnt);

				htmlMsg.append("</body></html>");
				eml.setHtmlMsg(htmlMsg.toString());
				EmailAttachment attachment = new EmailAttachment();
				attachment.setPath(pdfBothReportName);
				attachment.setDisposition(EmailAttachment.ATTACHMENT);
				eml.attach(attachment);
				if (type.endsWith("Daily")) {
					EmailAttachment attachment2 = new EmailAttachment();
					attachment2.setPath(estimatedPriceCSVFile.getPath());
					attachment2.setDisposition(EmailAttachment.ATTACHMENT);
					eml.attach(attachment2);
				}
				eml.setHostName("mcverryreport.com");
				eml.setSmtpPort(465);
				eml.setSSL(true);
				eml.setAuthentication("subscriptions@mcverryreport.com", "n4rlyW00D$");
				String emlret = eml.send();
				logr.info(rsmail.getString(1) + ":" + emlret);
				updatemail.setString(1, currentMktDate);
				updatemail.setInt(2, idx);
				updatemail.execute();

			}
		}

		if (debugging)
			;
		else {
			webPageUpdate(currentMktDate);
			ReportForStockTwits rfst = new ReportForStockTwits();
			rfst.get(day5, day10, day15);
		}

		PrintWriter pwThreadAvg = new PrintWriter(
				"c:/users/joe/correlationOutput/threadRunAverages_" + currentMktDate + csvFileType);
		for (String key : threadRunAverages.keySet()) {
			pwThreadAvg.println(key + ";" + threadRunAverages.get(key).get());
		}
		pwThreadAvg.flush();
		pwThreadAvg.close();

		PreparedStatement makeAcctInactive = connRemote
				.prepareStatement("update subscribers set status = 'Inactive' where status = 'Active' and count < 1");
		makeAcctInactive.execute();

		estimatedPriceCSVPW.close();
	}

	static public void topReport(boolean starter, StringBuffer emailText, TopAndBottomList catOverallAverages,
			TopAndBottomList tabLookAhead, TreeMap<String, StringBuffer> catETFList) {

		if (starter == false)
			emailText.append("<p>");
		boolean contrast = ThreadLocalRandom.current().nextBoolean();
		if (catOverallAverages.getTopValue(0) > 5.5) {
			if (starter)
				emailText.append("The ");
			else
				emailText.append((contrast ? "In contrast" : "On the Bullish side") + ", the ");

			emailText.append(wordsCapitalized(catOverallAverages.getTopDescription()[0]) + " sector has a positive "
					+ " forecast for the next 30 trading days, with an average score of "
					+ df.format(catOverallAverages.getTopValue(0)) + ". ");
		}
		String besties[] = tabLookAhead.getTopDescription()[0].split("<");
		boolean bestOrTop = ThreadLocalRandom.current().nextBoolean();
		if (besties[0].compareTo(catOverallAverages.getTopDescription()[0]) == 0
				& catOverallAverages.getTopValue(0) > 5.5) {
			emailText.append("And the ");
		} else {
			emailText.append("The ");

		}

		emailText.append(wordsCapitalized(besties[0]) + " sector has " + (bestOrTop ? "the best" : "a good"));
		bestOrTop = ThreadLocalRandom.current().nextBoolean();
		emailText.append(" short-term " + (bestOrTop ? "prediction" : "prospect") + ", which should occur in "
				+ besties[1] + " days. ");

		emailText.append("The computed average is " + df.format(tabLookAhead.getTopValue(0)) + ". ");

		String catSyms[] = catETFList.get(besties[0]).toString().split(" ");
		TopAndBottomList topsym = new TopAndBottomList();
		String periods[] = besties[1].split(" ");
		int p1 = Integer.parseInt(periods[0]);
		int p2 = Integer.parseInt(periods[2]);

		for (String csym : catSyms) {
			double avg[] = symbolDailyAverages.get(csym);
			if (avg == null)
				continue;
			for (int i = p1; i <= p2; i++) {
				topsym.setTop(avg[i], csym);
			}
		}

		bestOrTop = ThreadLocalRandom.current().nextBoolean();
		emailText.append(
				" The estimated " + (bestOrTop ? "best" : "top") + " ETF in that " + (bestOrTop ? "sector" : "category")
						+ " for that period is $" + topsym.getTopDescription()[0].toUpperCase() + ". ");

		for (int i = 1; i < 10; i++) {
			if (tabLookAhead.getTopDescription()[i].startsWith(besties[0]) == false
					& tabLookAhead.getTopValue(i) > 4.5) {
				besties = tabLookAhead.getTopDescription()[i].split("<");
				periods = besties[1].split(" ");
				int p11 = Integer.parseInt(periods[0]);
				int p21 = Integer.parseInt(periods[2]);
				topsym = new TopAndBottomList();
				besties = tabLookAhead.getTopDescription()[i].split("<");
				bestOrTop = ThreadLocalRandom.current().nextBoolean();
				emailText.append(" Next is followed by the " + wordsCapitalized(besties[0]) + " sector with a "
						+ (bestOrTop ? "good" : "better than average") + " forecast");
				if (p11 == p1 & p21 == p2)
					emailText.append(" for the same time frame. ");
				else
					emailText.append(" in " + besties[1] + " days. ");
				emailText.append("The computed average is " + df.format(tabLookAhead.getTopValue(i)) + ". ");
				catSyms = catETFList.get(besties[0]).toString().split(" ");
				for (String csym : catSyms) {
					double avg[] = symbolDailyAverages.get(csym);
					if (avg == null)
						continue;
					for (i = p1; i <= p2; i++) {
						topsym.setTop(avg[i], csym);
					}
				}
				bestOrTop = ThreadLocalRandom.current().nextBoolean();
				emailText.append(
						" The estimated " + (bestOrTop ? "best" : "top") + " ETF in this sector for the period is $"
								+ topsym.getTopDescription()[0].toUpperCase() + ". ");

				break;
			}
		}

	}

	static void bottomReport(boolean starter, StringBuffer emailText, TopAndBottomList catOverallAverages,
			TopAndBottomList tabLookAhead, TreeMap<String, StringBuffer> catETFList) {

		if (starter == false)
			emailText.append("<p>");
		if (catOverallAverages.getBottomValue(0) < 3.5) {
			if (starter)
				emailText.append("The ");
			else
				emailText.append("In contrast, the ");
			emailText.append(wordsCapitalized(catOverallAverages.getBottomDescription()[0]) + " sector has a bearish "
					+ " forecast for the next 30 trading days, with an average score of "
					+ df.format(catOverallAverages.getBottomValue(0)) + ". ");
		}

		boolean worseOrLowest = ThreadLocalRandom.current().nextBoolean();
		String besties[] = tabLookAhead.getBottomDescription()[0].split("<");
		if (besties[0].compareTo(catOverallAverages.getBottomDescription()[0]) == 0
				& catOverallAverages.getBottomValue(0) < 3.5) {
			emailText.append("Also, the ");
		} else {
			emailText.append("The ");

		}

		emailText.append(
				wordsCapitalized(besties[0]) + " sector has " + (worseOrLowest ? "a poor" : "an underperforming")
						+ " prospect in the " + besties[1] + " day period; ");

		emailText.append("the computed average is " + df.format(tabLookAhead.getBottomValue(0)) + ".");

		TopAndBottomList topsym = new TopAndBottomList();
		String periods[] = besties[1].split(" ");
		int p1 = Integer.parseInt(periods[0]);
		int p2 = Integer.parseInt(periods[2]);
		String catSyms[] = catETFList.get(besties[0]).toString().split(" ");
		for (String csym : catSyms) {
			double avg[] = symbolDailyAverages.get(csym);
			if (avg == null)
				continue;
			for (int i = p1; i <= p2; i++) {
				topsym.setBottom(avg[i], csym);
			}
		}
		worseOrLowest = ThreadLocalRandom.current().nextBoolean();

		emailText.append(" In this sector, $" + topsym.getBottomDescription()[0].toUpperCase() + " may have the "
				+ (worseOrLowest ? "worst" : "lowest") + " results. ");

		for (int i = 1; i < 10; i++) {
			if (tabLookAhead.getBottomDescription()[i].startsWith(besties[0]) == false
					& tabLookAhead.getBottomValue(i) < 4.5) {
				besties = tabLookAhead.getBottomDescription()[i].split("<");
				periods = besties[1].split(" ");
				int p11 = Integer.parseInt(periods[0]);
				int p21 = Integer.parseInt(periods[2]);
				emailText.append(
						" Coming in second with a weak prospect is the " + wordsCapitalized(besties[0]) + " sector ");
				if (p11 == p1 & p21 == p2)
					emailText.append("for the same period. ");
				else
					emailText.append("in " + besties[1] + " days.");
				emailText.append(" The computed average is " + df.format(tabLookAhead.getBottomValue(i)) + ".");
				topsym = new TopAndBottomList();
				catSyms = catETFList.get(besties[0]).toString().split(" ");
				for (String csym : catSyms) {
					double avg[] = symbolDailyAverages.get(csym);
					if (avg == null)
						continue;
					for (i = p1; i <= p2; i++) {
						topsym.setBottom(avg[i], csym);
					}
				}
				worseOrLowest = ThreadLocalRandom.current().nextBoolean();
				emailText.append(" The models expect the ETF $" + topsym.getBottomDescription()[0].toUpperCase()
						+ " to finish the " + (worseOrLowest ? "worst" : "lowest") + " in this group. ");
				break;
			}
		}

	}

	static String wordsCapitalized(String in) {
		StringBuffer sb = new StringBuffer(1000);
		String words[] = in.split(" ");
		for (String word : words)
			if (word.compareTo("us") == 0)
				sb.append("U.S. ");
			else if (word.compareTo("non-us") == 0)
				sb.append("non-U.S. ");
			else
				sb.append(StringUtils.capitalize(word) + " ");
		return sb.toString().trim();
	}

	static String formatPDFCell(double value) {

//		static String cellColors[] = { "red", "#FFA500", "#ffa534", "#FAFAD2", "#F0FFF0", "#F0F8FF", "#b5dbc2", "#69ffa5",
//				"#7cf600", "green" };

		Double dv = value;
		String rpl;

		if (value <= 4.5) {
			dv = 255 * (value / 4.5);
			int hx = dv.intValue();
			rpl = String.format("#FF%02X%02X", hx, hx);
		} else {
			dv = 255 * ((9 - value) / 4.5);
			int hx = dv.intValue();
			rpl = String.format("#%02XFF%02X", hx, hx);

		}
		String ret = pdfDataCell.replace("%c", rpl);

		ret = ret.replace("%s", String.format("%1.1f", value));
		return ret;
	}

	Connection conn;
	String function;

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public CorrelationEstimator(Connection conn) {
		this.conn = conn;
	}

	String sym;

	TreeMap<String, Integer> functionDaysDiffMap = null;
	TreeMap<String, Integer> doubleBacks = null;
	TreeMap<String, String[]> dates = null;
	int daysOut;
	String startDate;

	DeltaBands priceBands;

	static String currentMktDate;
	TreeMap<String, Object> myParms;

	TreeMap<String, Double> theBadness;
	boolean whichHalf = false;

	Semaphore threadSemaphore;

	public void setWork(String sym, int daysOut, DeltaBands priceBands, TreeMap<Integer, Averager> smiaverageForDaysOut,
			TreeMap<String, Double> theBadness, boolean whichHalf, Semaphore threadSemaphore) throws Exception {

		this.sym = sym;
		this.daysOut = daysOut;

		myParms = new TreeMap<>();
		functionDaysDiffMap = new TreeMap<>();
		doubleBacks = new TreeMap<>();
		dates = new TreeMap<>();

		this.priceBands = priceBands;

		this.avgForDaysOut = smiaverageForDaysOut;

		this.theBadness = theBadness;
		this.whichHalf = whichHalf;
		this.threadSemaphore = threadSemaphore;

	}

	static TreeMap<String, Averager> threadRunAverages = new TreeMap<>();

	@Override
	public void run() {

		Instant start = Instant.now();
		try {
			double got = prun();

			if (Double.isNaN(got)) {
				System.err.println("found a bad one ");
				return;
			}
			setResultsForDB(got);

			theBadness.put(function + ";" + daysOut, got);
			synchronized (theBadness) {
				Averager thisDaysAverage = avgForDaysOut.get(daysOut);
				if (thisDaysAverage == null) {
					thisDaysAverage = new Averager();
					avgForDaysOut.put(daysOut, thisDaysAverage);
				}
				// System.out.println("\n"+this.getClass().getSimpleName() + ";" + got);
				thisDaysAverage.add(got);

			}

		} catch (

		Exception e) {
			e.printStackTrace();
			System.exit(0);
			return;
		}

		Instant finish = Instant.now();
		String key = function + ":" + daysOut;
		synchronized (threadRunAverages) {
			Averager avg = threadRunAverages.get(key);
			if (avg == null) {
				avg = new Averager();
				threadRunAverages.put(key, avg);
			}
			// System.out.println(1. * Duration.between(start, finish).toMillis());

			avg.add(1. * Duration.between(start, finish).toMillis());
		}

		threadSemaphore.release();
	}

	public double prun() throws Exception {
		AttributeParm parms = null;
		try {
			parms = makeSQL.buildParameters(sym, daysOut, conn);
		} catch (Exception e1) {
			e1.printStackTrace();
			System.exit(0);
		}
		String $instances = null;
		try {

			File instanceFile = new File("c:/users/joe/correlationARFF/" + function + "/" + sym + "/D" + daysOut
					+ (whichHalf ? "f" : "s") + ".zrff");

			if (instanceFile.exists()) {
				synchronized (conn) {
					/*
					 * BufferedReader br = new BufferedReader(new FileReader(instanceFile));
					 * StringBuilder builder = new StringBuilder(250000); char[] buffer = new
					 * char[4096]; int numChars; while ((numChars = br.read(buffer)) > 0) {
					 * builder.append(buffer, 0, numChars); } byte[] output =
					 * builder.toString().getBytes(StandardCharsets.UTF_8);br.close(); builder = new
					 * StringBuilder(250000);
					 */
					byte[] output = Files.readAllBytes(instanceFile.toPath());
					Inflater inflater = new Inflater();
					inflater.setInput(output);
					StringBuilder builder = new StringBuilder(250000);
					while (inflater.finished() == false) {
						byte b100[] = new byte[100];
						int len2 = inflater.inflate(b100);
						builder.append(new String(b100, 0, len2, "Cp1252"));
					}
					inflater.end();
					$instances = builder.toString();
				}
			} else {
				$instances = buildInstances(parms);
				byte[] bytes = $instances.getBytes("Cp1252");
				byte[] output = new byte[bytes.length / 2];
				Deflater deflater = new Deflater();
				deflater.setInput(bytes);
				deflater.finish();
				int len = deflater.deflate(output);
				File dirFile = new File("c:/users/joe/correlationARFF/" + function);
				if (dirFile.exists() == false) {
//					dirFile.createNewFile();
					dirFile.mkdir();
				}
				dirFile = new File("c:/users/joe/correlationARFF/" + function + "/" + sym);
				if (dirFile.exists() == false) {
//					dirFile.createNewFile();
					dirFile.mkdir();
				}
				instanceFile.createNewFile();
				Files.write(instanceFile.toPath(), output);

			}
		} catch (Exception e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		if ($instances == null) {
			System.out.println("if ($instances == null)");
			System.exit(0);
		}

		String $lastLine = null;
		try {

			$lastLine = makeSQL.makeARFFFromSQLForQuestionMark(sym, daysOut, parms);

		} catch (

		Exception e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		if ($lastLine == null) {
			System.out.println("if ($lastLine == null)");
			System.exit(0);
		}

		Instances trainInst;
		trainInst = new Instances(new StringReader($instances));
		trainInst.setClassIndex(trainInst.numAttributes() - 1);

		Classifier classifier = new IBk();

		classifier.buildClassifier(trainInst);

		Instances testInst;
		testInst = new Instances(new StringReader($lastLine));
		testInst.setClassIndex(testInst.numAttributes() - 1);
		return classifier.classifyInstance(testInst.get(testInst.size() - 1));
	}

	public static void setResultsForDBForBadNess(double got, String sym, int daysOut) {
		if (resultsForDBPrintWriter != null)
			try {
				resultsForDBPrintWriter.println(sym + ";Bad;" + currentMktDate + ";" + daysOut + ";" + got);

			} catch (Exception e) {

				e.printStackTrace();
				System.exit(0);
			}

	}

	public void setResultsForDB(double got) {
		if (resultsForDBPrintWriter != null)
			try {
				resultsForDBPrintWriter
						.println(sym + ";" + function + ";" + currentMktDate + ";" + daysOut + ";" + got);

			} catch (Exception e) {

				e.printStackTrace();
				System.exit(0);
			}

	}

	public static void webPageUpdate(String lastMktDate) {
		if (debugging) {
			System.out.println("debugging, so web page is not updated.");
			return;

		}
		StringWriter swWebpage = new StringWriter(500);
		PrintWriter pwWebPage = new PrintWriter(swWebpage/* "pointerToEstimates.htmx" */);

		pwWebPage.println("<div style='font-size:125%;'>ETF Expectations&#8482;</div>");
		pwWebPage.println(
				"<div style='font-size:107%;'>The Top ETFs With The Strongest<br>Predicted Price Movement Indications</div>");
		pwWebPage.println("<div style='font-size:110%;'>For The Next 6 Trading Weeks. </div>");
		pwWebPage.println("<div style='font-size:107%;'>&mdash;\r\n as of " + lastMktDate + "</div>");

		if (B5.isEmpty() == false | S5.isEmpty() == false)
			pwWebPage.println("<hr width='50%'><div style='font-size:105%'>1 Week &mdash;" + " Latest Picks</div>");
		if (B5.isEmpty() == false) {
			pwWebPage.print(" Buy:");
			Iterator<String> iter = B5.iterator();

			while (iter.hasNext()) {
				pwWebPage.print(" $" + iter.next());

			}
			pwWebPage.println("<br>");
		}

		if (S5.isEmpty() == false) {
			pwWebPage.print(" Bearish:");
			Iterator<String> iter = S5.iterator();

			while (iter.hasNext()) {
				pwWebPage.print(" $" + iter.next());

			}
			pwWebPage.println("<br>");

		}

		if (B10.isEmpty() == false | S10.isEmpty() == false)
			pwWebPage.println("<hr width='50%'><div style='font-size:105%'>2 Week &mdash;" + " Latest Picks</div>");
		if (B10.isEmpty() == false) {
			pwWebPage.print(" Bullish:");
			Iterator<String> iter = B10.iterator();

			while (iter.hasNext()) {
				pwWebPage.print(" $" + iter.next());

			}
			pwWebPage.println("<br>");
		}

		if (S10.isEmpty() == false) {
			pwWebPage.print(" Bearish:");
			Iterator<String> iter = S10.iterator();

			while (iter.hasNext()) {
				pwWebPage.print(" $" + iter.next());

			}
			pwWebPage.println("<br>");

		}

		pwWebPage.print("<a href='cgi/aiEstimates.php'><div style='font-size:112%'>See other picks here...</div></a>");
		pwWebPage.flush();
		pwWebPage.close();

		try {

			FTPClient ftpc = new FTPClient();
			ftpc.connect("mcverryreport.com");

			ftpc.login("reportly@mcverryreport.com", "*gg77_3*g168");

			ftpc.setControlKeepAliveTimeout(300);
			ftpc.enterLocalPassiveMode();
			ftpc.storeFile("pointerToEstimates.htmx", new ByteArrayInputStream(swWebpage.toString().getBytes()));

//		ftpc.storeFile("pointerToEstimates.htmx", new FileInputStream("pointerToEstimates.htmx"));
			ftpc.disconnect();

			pwWebPage = new PrintWriter("pointerToEstimates.htmx");
			pwWebPage.print(swWebpage.toString());
			pwWebPage.flush();
			pwWebPage.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static ArrayList<String> getWhatWasPrintedYesterday(Connection connRemote, String previousMarketDate)
			throws SQLException {

		ArrayList<String> ret = new ArrayList<>();
		PreparedStatement getYesterday = connRemote
				.prepareStatement("select currentReport from forecastReport   where dateOfReport  = ?");
		getYesterday.setString(1, previousMarketDate);
		ResultSet rs = getYesterday.executeQuery();
		if (rs.next() == false)
			return ret;

		String rptData = rs.getString(1);
		if (rptData.length() < 1)
			return ret;
		rptData = rptData.replaceAll("<tr>", "");
		rptData = rptData.replaceAll("</tr>", "\n");
		String rptDatas[] = rptData.split("\n");

		for (String rd : rptDatas) {

			rd = rd.replaceAll("<td>", "");
			rd = rd.replaceAll("</td>", "-\n");
			String rds[] = rd.split("\n");
			String sym = rds[0].replace("-", "");

			{
				if (rds[1].contains("td1-") & rds[1].contains("td2-"))
					;
				else
					ret.add(sym);

			}
		}
		return ret;
	}

	public static String doHistory(Connection connRemote, ArrayList<String> datesApart, String todaysDate)
			throws Exception {

		PreparedStatement historyGet = connRemote
				.prepareStatement("select currentReport from forecastReport where dateOfReport  = ?");

		StringBuffer histPDF = new StringBuffer(1000);

		for (int da = 0; da < datesApart.size(); da++) {
			historyGet.setString(1, datesApart.get(da));
			ResultSet rsHistoryGet = historyGet.executeQuery();
			if (rsHistoryGet.next()) {

				String rptData = rsHistoryGet.getString(1);
				rptData = rptData.replaceAll("<tr>", "");
				rptData = rptData.replaceAll("</tr>", "\n");
				String rptDatas[] = rptData.split("\n");
				Averager buyAverage = new Averager();
				Averager sellAverage = new Averager();
				Averager allAverage = new Averager();

				getRptDatas: for (String rd : rptDatas) {
					rd = rd.replaceAll("<td>", "");
					rd = rd.replaceAll("</td>", "-\n");
					String rds[] = rd.split("\n");
					if (rds.length < 2)
						continue;
					String sym = rds[0].replace("-", "");

					String compBorS = "td" + (da + 1);
					String dat = rds[1];
					if (dat.contains(compBorS + "-"))
						continue;
					GetETFDataUsingSQL gsd;
					try {
						gsd = GetETFDataUsingSQL.getInstance(sym);
					} catch (GSDException gsde) {
						System.out.println(gsde.getLocalizedMessage());
						continue getRptDatas;
					}

					double startPrice = gsd.inClose[gsd.inClose.length - ((da * 5) + 6)];
					Averager adiff = new Averager();
					for (int dai = 1; dai <= 5; dai++) {

						double compPrice = gsd.inClose[gsd.inClose.length - dai]; // the most current 5 days
						adiff.add(compPrice - startPrice);
					}

					double diff = adiff.get();

					if (rds[1].contains(compBorS + "B")) {
						if (diff > 0) {
							buyAverage.add(1);
							allAverage.add(1);
						} else {
							buyAverage.add(0);
							allAverage.add(0);

						}
					} else if (rds[1].contains(compBorS + "S")) {
						if (diff < 0) {

							sellAverage.add(1);
							allAverage.add(1);
						} else {
							sellAverage.add(0);
							allAverage.add(0);

						}

					}

				}

				String buyAvg$ = "n/c";
				String sellAvg$ = "n/c";
				String allAvg$ = "n/c";
				if (buyAverage.getCount() > 0)
					buyAvg$ = df.format(buyAverage.get() * 100) + "&percnt;";

				if (sellAverage.getCount() > 0)
					sellAvg$ = df.format(sellAverage.get() * 100) + "&percnt;";

				if (allAverage.getCount() > 0)
					allAvg$ = df.format(allAverage.get() * 100) + "&percnt;";

				histPDF.append(pdfStartRowWithBorder);

				String updateStatement = "update forecastReport  set " + (da + 1) * 5 + "DayResult='" + "Call Made on "
						+ datesApart.get(da) + "<br>Bullish Correct Average: " + buyAvg$
						+ "<br>Bearish Correct Average: " + sellAvg$ + "<br>Both Calls Correct Average: " + allAvg$
						+ "<form method=\"post\" action=\"aiEstimates.php\"> "
						+ "<input type=\"submit\" name=\"goto\" value=\"See Report\" id=\"goto\"/> "
						+ "<input type=\"hidden\" name=\"rowid\" value=\"1\"/> "
						+ "<input type=\"hidden\" name=\"dateOfReport\" value=\"" + datesApart.get(da) + "\"> "
						+ "</form>" + "' where dateOfReport = '" + todaysDate + "'";
				PreparedStatement updatePS = connRemote.prepareStatement(updateStatement);
				if (debugging)
					/* System.out.println("history not updated") */;
				else
					updatePS.execute();
				switch (da) {
				case 0:
					histPDF.append(pdfSymbolCell.replace("%s", "1 - 5 Days"));
					break;

				case 1:
					histPDF.append(pdfSymbolCell.replace("%s", "6 - 10 Days"));
					break;

				case 2:
					histPDF.append(pdfSymbolCell.replace("%s", "11 - 15 Days"));
					break;

				case 3:
					histPDF.append(pdfSymbolCell.replace("%s", "16 - 20 Days"));
					break;

				case 4:
					histPDF.append(pdfSymbolCell.replace("%s", "21 - 25 Days"));
					break;

				case 5:
					histPDF.append(pdfSymbolCell.replace("%s", "26 - 30 Days"));
					break;

				default:
					throw new Exception("logic error on days out with a value of " + da);

				}

				String resultsForPdf = "Call Made on " + datesApart.get(da)
						+ "</fo:block><fo:block>Bullish Correct Average: " + buyAvg$.replace("&percnt;", "%")
						+ "</fo:block><fo:block>Bearish Correct Average: " + sellAvg$.replace("&percnt;", "%")
						+ "</fo:block><fo:block>Both Calls Correct Average: " + allAvg$.replace("&percnt;", "%") + "\n";

				histPDF.append(pdfSymbolCell.replace("%s", resultsForPdf));

				histPDF.append(pdfEndRow);
			}
		}

		return histPDF.toString();
	}

	public static void updateResultsTable(Connection conn, File resultsForDBFile2) throws Exception {

		String debugAppend = "";
		if (debugging) {
			debugAppend = "_debug";
		}
		BufferedReader br = new BufferedReader(new FileReader(resultsForDBFile2));
		String in;
		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			String ins[] = in.split(";");

			PreparedStatement updateFunctionResults = conn.prepareStatement("insert into correlationfunctionresults"
					+ debugAppend + " (symbol, function, mktDate, daysOut, guess) " + " values(?,?,?,?,?) "
					+ " on duplicate key update guess=? ");
			updateFunctionResults.setString(1, ins[0]);
			updateFunctionResults.setString(2, ins[1]);
			updateFunctionResults.setString(3, ins[2]);
			updateFunctionResults.setInt(4, Integer.parseInt(ins[3]));
			updateFunctionResults.setDouble(5, Double.parseDouble(ins[4]));
			updateFunctionResults.setDouble(6, Double.parseDouble(ins[4]));
			updateFunctionResults.execute();
		}
		conn.commit();
		conn.setAutoCommit(true);
		br.close();
	}

	static boolean isTodayLastDayOfWeek() {

		Calendar clg = Calendar.getInstance();
		if (clg.get(Calendar.DAY_OF_WEEK) == Calendar.FRIDAY)
			return true;
		if (clg.get(Calendar.DAY_OF_WEEK) != Calendar.THURSDAY)
			return false;

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		clg.add(Calendar.DAY_OF_MONTH, +1);
		String dtfmt = df.format(clg.getTime());

		try {
			BufferedReader br = new BufferedReader(new FileReader("holidays.txt"));
			String holidate = "";
			while ((holidate = br.readLine()) != null) {
				holidate = holidate.trim();
				if (holidate.startsWith("#")) {
					continue;
				}
				if (holidate.compareTo(dtfmt) == 0) {
					return true;
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		return false;

	}

	public String buildInstances(AttributeParm parms) {
		try {

			return makeSQL.makeARFFFromSQL(sym, daysOut, parms, whichHalf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
