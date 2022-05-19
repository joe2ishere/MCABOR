package correlation.Estimators;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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
import correlation.PDFReport;
import correlation.PerformanceFromDBForPDF;
import correlation.ReportForStockTwits;
import correlation.UpdateResults;
import correlation.Estimators.CorrelationDoBad.CDBRecord;
import correlation.Estimators.CorrelationDoWork.CEWork;
import correlation.Estimators.Runners.APOCorrelationEstimator;
import correlation.Estimators.Runners.ATRCorrelationEstimator;
import correlation.Estimators.Runners.CorrelationEstimatorRunner;
import correlation.Estimators.Runners.DMICorrelationEstimator;
import correlation.Estimators.Runners.MAAveragerCorrelationEstimator;
import correlation.Estimators.Runners.MACDCorrelationEstimator;
import correlation.Estimators.Runners.MACorrelationEstimator;
import correlation.Estimators.Runners.MALinesCorrelationEstimator;
import correlation.Estimators.Runners.PSARCorrelationEstimator;
import correlation.Estimators.Runners.SMICorrelationEstimator;
import correlation.Estimators.Runners.TSFCorrelationEstimator;
import util.Averager;
import util.DateLine;
import util.getDatabaseConnection;
import utils.StandardDeviation;
import utils.TopAndBottomList;

public abstract class CorrelationEstimator {
	static DecimalFormat df = new DecimalFormat("#.00");
	static DecimalFormat df4 = new DecimalFormat("#.####");

	static ArrayList<String> B5 = new ArrayList<>();
	static ArrayList<String> S5 = new ArrayList<>();
	static ArrayList<String> B10 = new ArrayList<>();
	static ArrayList<String> S10 = new ArrayList<>();

	public static TreeMap<String, GetETFDataUsingSQL> gsds = new TreeMap<>();
	static ArrayList<String> primaryETFs = new ArrayList<String>();

	TreeMap<Integer, StandardDeviation> avgForDaysOut;

	static TreeMap<Integer, Favored> dailyHighlyFavored = new TreeMap<>();
	static TreeMap<Integer, Favored> dailyLeastFavored = new TreeMap<>();
	static ArrayList<String> doneList;

	static String pdfStartRowWithThinBorder = "<fo:table-row border='0.07px solid lightgrey'>";
	static String pdfStartRowWithBorder = "<fo:table-row border='.25px solid black'>";

	static String pdfSymbolCell = "<fo:table-cell ><fo:block>%s</fo:block></fo:table-cell>\n";
	static String pdfEndRow = "</fo:table-row>";
	static String pdfDataCell = "<fo:table-cell><fo:block background-color='%c'>%s</fo:block></fo:table-cell>\n";

	public static double buyIndicatorLimit = 7.25;
	public static double sellIndicatorLimit = 1.75;

	static int queueCnt = 4;

	static File resultsForDBFile;
	static PrintWriter resultsForDBPrintWriter;

	static String updateOldFileDate = null;

	static boolean debugging = true;
	static boolean debuggingFast = false;
	static boolean doA130DayRun = false;

	static TreeMap<String, double[]> symbolDailyAverages = new TreeMap<>();

	public static TreeMap<String, ArrayList<Averager>> functionDayAverager;

	static boolean doingElite = false;

	static String csvFileType;

	public void setDoElite() {
		doingElite = true;
	}

	public static void loadGSDSTable(Connection conn) throws Exception {
		PreparedStatement selectSymbols = conn.prepareStatement("""
				 SELECT DISTINCT symbol FROM prices WHERE symbol IN
				(SELECT DISTINCT symbol FROM dmi_correlation) OR
				symbol IN (SELECT DISTINCT functionsymbol  FROM dmi_correlation) OR
				symbol IN (SELECT DISTINCT symbol FROM macd_correlation) OR
				symbol IN (SELECT DISTINCT functionsymbol  FROM macd_correlation) OR
				symbol IN (SELECT DISTINCT symbol FROM sm_correlation) OR
				symbol IN (SELECT DISTINCT functionsymbol  FROM sm_correlation) OR
				symbol IN (SELECT DISTINCT symbol FROM tsf_correlation) OR
				symbol IN (SELECT DISTINCT functionsymbol  FROM tsf_correlation) order by symbol
				""");
		ResultSet rsSymbols = selectSymbols.executeQuery();
		while (rsSymbols.next()) {
			String sym = rsSymbols.getString(1);
			GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
			gsds.put(sym, gsd);
		}
		String selectStatement = (doA130DayRun == false ? """
						SELECT DISTINCT symbol FROM dmi_correlation WHERE symbol IN
						(SELECT DISTINCT symbol FROM macd_correlation) and
						symbol IN (SELECT DISTINCT symbol FROM sm_correlation) and
						symbol IN (SELECT DISTINCT symbol FROM tsf_correlation)
				""" : """
				SELECT DISTINCT symbol FROM dmi_correlation_130 WHERE symbol IN
				(SELECT DISTINCT symbol FROM macd_correlation_130) and
				symbol IN (SELECT DISTINCT symbol FROM sm_correlation_130) and
				symbol IN (SELECT DISTINCT symbol FROM tsf_correlation_130)
				""");

		selectSymbols = conn.prepareStatement(selectStatement);

		rsSymbols = selectSymbols.executeQuery();
		while (rsSymbols.next()) {
			String sym = rsSymbols.getString(1);
			primaryETFs.add(sym);
		}
		selectSymbols.close();
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
					csvFileType = "Debug.csv";
					System.out.println("(F)ast or (S)low");
					String debugFastOrSlow = br.readLine() + " ";
					if (debugFastOrSlow.startsWith("F")) {
						debuggingFast = true;
						break;
					}

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

//		if (debugging)
//			functionDayAverager = CorrelationFunctionPerformance.loadFromFile();
//		else
//			functionDayAverager = CorrelationFunctionPerformance.getFunctionDayAverage("");

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

		File mailMessageFile = new File("c:/users/joe/correlationOutput/mailMessage.txt");
		File reportXMLFOFile = new File("xmlFilesForPDFReports/Report-FO.xml");

		resultsForDBFile = new File("c:/users/joe/correlationOutput/resultsForFunctionTest_"
				+ (doA130DayRun == false ? "" : "130_") + currentMktDate + csvFileType);

		resultsForDBPrintWriter = new PrintWriter(resultsForDBFile);
		PrintWriter averageOutPW = new PrintWriter("c:/users/joe/correlationOutput/"
				+ (doA130DayRun == false ? "" : "130_") + currentMktDate + csvFileType);

		File rawDataCSVFile = new File("c:/users/joe/correlationOutput/rawData" + (doA130DayRun == false ? "" : "130_")
				+ currentMktDate + csvFileType);
		PrintWriter rawDataCSVPW = new PrintWriter(rawDataCSVFile);

		File differentialCSVFile = new File("c:/users/joe/correlationOutput/differntial"
				+ (doA130DayRun == false ? "" : "130_") + currentMktDate + csvFileType);
		PrintWriter differentialCSVPW = new PrintWriter(differentialCSVFile);

		File estimatedPriceChangeCSVFile = new File("c:/users/joe/correlationOutput/estimatedPercentageChanges"
				+ (doA130DayRun == false ? "" : "130_") + currentMktDate + csvFileType);
		PrintWriter estimatedPriceChangeCSVPW = new PrintWriter(estimatedPriceChangeCSVFile);

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
			double[] symavg = new double[doA130DayRun == false ? 31 : 131];
			symbolDailyAverages.put(sym, symavg);
			if (datesApart == null) {
				datesApart = new ArrayList<>();
				for (int i = 5; i <= (doA130DayRun == false ? 30 : 130); i += 5) {
					datesApart.add(gsd.inDate[gsd.inDate.length - (i + 1)]);
					if (i == (doA130DayRun == false ? 25 : 125))
						updateOldFileDate = gsd.inDate[gsd.inDate.length - (i + 1)];
				}
			}
		}

		// System.exit(0);

		TreeMap<String, TreeMap<Integer, Averager>> catAverages = new TreeMap<>();
		TreeMap<String, StringBuilder> catETFList = new TreeMap<>();
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
				catETFList.put(cat, new StringBuilder(1000));
			}
			symCats.put(sym, cat);
			StringBuilder catList = catETFList.get(cat);
			catList.append(sym + " ");
		}

		final PreparedStatement selectDays = conn.prepareStatement("select distinct toCloseDays from tsf_correlation"
				+ (doA130DayRun == false ? "" : "_130") + " order by toCloseDays");
		ResultSet rsSelectDays = selectDays.executeQuery();
		ArrayList<Integer> daysOutList = new ArrayList<Integer>();
		System.out.print("Daily symbol");
		averageOutPW.print("Daily");
		rawDataCSVPW.print("Symbol");
		differentialCSVPW.print("Symbol");
		estimatedPriceChangeCSVPW.print("Symbol");

		String hdgDate = DateLine.dateLine(currentMktDate, doA130DayRun == false ? 30 : 130);
		averageOutPW.print(hdgDate);
		rawDataCSVPW.print(hdgDate);
		differentialCSVPW.print(hdgDate);
		estimatedPriceChangeCSVPW.print(hdgDate);
		System.out.print(hdgDate);

		while (rsSelectDays.next()) {
			int day = rsSelectDays.getInt(1);
			daysOutList.add(day);
			dailyHighlyFavored.put(day, new Favored("", -1.));
			dailyLeastFavored.put(day, new Favored("", 99.));

		}
		if (doA130DayRun == false) {
			System.out.println(";Avg. 1-5; Avg. 6-10; Avg. 11-15;Avg. 16-20; Avg. 21-25; Avg. 26-30;Avg.");
			averageOutPW.println(";Avg. 1-5; Avg. 6-10; Avg. 11-15;Avg. 16-20; Avg. 21-25; Avg. 26-30;Avg.");
			rawDataCSVPW.println();
			differentialCSVPW.println();
			estimatedPriceChangeCSVPW.println();
		} else
			System.out.println();

		ArrayList<CorrelationEstimatorRunner> estimators = new ArrayList<>();

		// TODO set estimators
		if (doA130DayRun == false) { // slowest first
			estimators.add(new SMICorrelationEstimator(conn, true));
			estimators.add(new ATRCorrelationEstimator(conn, true));
			estimators.add(new TSFCorrelationEstimator(conn, true));
			estimators.add(new MALinesCorrelationEstimator(conn));
			estimators.add(new APOCorrelationEstimator(conn, true));
			estimators.add(new MACorrelationEstimator(conn));
			estimators.add(new MAAveragerCorrelationEstimator(conn));
			estimators.add(new PSARCorrelationEstimator(conn, true));
			// estimators.add(new SMIandTSFCorrelationEstimator(conn));
			// estimators.add(new MACDCorrelationEstimator(conn, true));
			// estimators.add(new ATRandMACDCorrelationEstimator(conn));
			// estimators.add(new MACDandMACorrelationEstimator(conn));
			// estimators.add(new SMIandMACDCorrelationEstimator(conn));
			// estimators.add(new DMICorrelationEstimator(conn, true));

		} else {
			estimators.add(new DMICorrelationEstimator(conn, false));
			estimators.add(new MACDCorrelationEstimator(conn, false));
			estimators.add(new SMICorrelationEstimator(conn, false));
			estimators.add(new TSFCorrelationEstimator(conn, false));
		}
		// estimators.get(0).getClass().getCanonicalName();
		// TODO set estimators
		StringBuilder etfExpectations = new StringBuilder(2000);
		StringBuilder etfExpectationsAbridged = new StringBuilder(2000);
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
//					if (sym.startsWith("inda") == false)
//						continue;
				}
			}
			System.out.print(sym);
			averageOutPW.print(sym);
			rawDataCSVPW.print(sym);
			differentialCSVPW.print(sym);
			estimatedPriceChangeCSVPW.print(sym);

			psInsertToSQL.setString(1, sym);

			etfExpectations.append(pdfStartRowWithThinBorder);
			etfExpectations.append(pdfSymbolCell.replace("%s", sym));

			StringBuilder sqlBuffer = new StringBuilder(1000);

			TreeMap<Integer, StandardDeviation> avgForDaysOut = new TreeMap<>();

			TreeMap<Integer, Averager> catAvg = null;
			if (symCats.containsKey(sym))
				catAvg = catAverages.get(symCats.get(sym));

			TreeMap<String, Double> theBadness = new TreeMap<>();

			boolean[] biggerHalfFirst = { true, false };
			// whichGroup[] biggerHalfFirst = { whichGroup.BEGIN, whichGroup.END };

			BlockingQueue<CEWork> cequeue = new ArrayBlockingQueue<CorrelationDoWork.CEWork>(queueCnt);
			ArrayList<Thread> cethreads = new ArrayList<>();
			for (int i = 0; i < queueCnt; i++) {
				cethreads.add(new Thread(new CorrelationDoWork(cequeue)));
				cethreads.get(i).start();
			}
			for (int daysOut : daysOutList) {
				avgForDaysOut.put(daysOut, new StandardDeviation());
				DeltaBands priceBands = new DeltaBands(gsds.get(sym).inClose, daysOut);
				for (var whichHalf : biggerHalfFirst) {
					{
						for (CorrelationEstimatorRunner ce : estimators) {
							cequeue.put(new CEWork(ce, sym, daysOut, priceBands, avgForDaysOut, theBadness, whichHalf));

						}
					}
				}
			}
			for (int i = 0; i < queueCnt; i++) {
				cequeue.put(new CEWork(null, "STOP", -1, null, null, null, false));

			}
			for (int i = 0; i < queueCnt; i++) {
				cethreads.get(i).join();
			}
			if (doA130DayRun == false) {
				BlockingQueue<CDBRecord> cdbQueue = new ArrayBlockingQueue<CDBRecord>(queueCnt);
				ArrayList<Thread> threads = new ArrayList<>();
				for (int i = 0; i < queueCnt; i++) {
					threads.add(new Thread(new CorrelationDoBad(cdbQueue, resultsForDBPrintWriter, currentMktDate)));
					threads.get(i).start();
				}
				for (int daysOut : daysOutList) {
					cdbQueue.put(new CDBRecord(sym, daysOut, theBadness, 'a', avgForDaysOut.get(daysOut)));
					cdbQueue.put(new CDBRecord(sym, daysOut, theBadness, 'b', avgForDaysOut.get(daysOut)));
				}
				for (int i = 0; i < queueCnt; i++) {
					CDBRecord cdbreca = new CDBRecord("STOP", -1, null, 'z', null);
					cdbQueue.put(cdbreca);
				}
				for (int i = 0; i < queueCnt; i++) {
					threads.get(i).join();
				}
			}

			double savg[] = new double[doA130DayRun == false ? 31 : 131];
			/* if (thirtyDayMode == false) */ {
				// estimatedPriceChangeCSVPW.print(";" +
				// df.format(gsds.get(sym).inClose[gsds.get(sym).inClose.length - 1]));
			}
			for (Integer key : avgForDaysOut.keySet()) {
				double theAvg = avgForDaysOut.get(key).getMean();
				System.out.print(";" + df.format(theAvg));
				averageOutPW.print(";" + df.format(theAvg));
				rawDataCSVPW.print(";" + df.format(theAvg));
				differentialCSVPW.print(";" + df.format(avgForDaysOut.get(key).getStandardDeviation()));
				DeltaBands priceBands = new DeltaBands(gsds.get(sym).inClose, key);
				estimatedPriceChangeCSVPW.print(";" + df4.format(priceBands.getApproximiateValue(theAvg) * 100));
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
			if (doA130DayRun == true) {
				System.out.println();
				continue;
			}

			StringBuilder webSiteReportSB = new StringBuilder("<tr><td>" + sym + "</td>");
			StringBuilder etfExpectationReportWaitForBuyOrSell = new StringBuilder(pdfStartRowWithThinBorder);
			etfExpectationReportWaitForBuyOrSell.append(pdfSymbolCell.replace("%s", sym));

			psInsertToSQL.setString(2, sqlBuffer.toString());
			psInsertToSQL.setString(3, sqlBuffer.toString());
			if (debugging) {
				System.out.println("debugging, so psInsertToSQL is not run.");
			} else {
				psInsertToSQL.execute();
			}

			double averages[] = new double[doA130DayRun == false ? 30 : 130];
			for (Integer daysOut : avgForDaysOut.keySet()) {
				averages[daysOut - 1] = avgForDaysOut.get(daysOut).getMean();
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
			differentialCSVPW.println();
			estimatedPriceChangeCSVPW.println();
			averageOutPW.flush();
			rawDataCSVPW.flush();
			differentialCSVPW.flush();
			estimatedPriceChangeCSVPW.flush();
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

		if (doA130DayRun == true) {
			estimatedPriceChangeCSVPW.close();
			differentialCSVPW.close();
			rawDataCSVPW.close();
			averageOutPW.close();
			return;
		}
		// makeBestWorstDaily
		StringBuilder bestDailyTop = new StringBuilder(1000);
		StringBuilder bestDailyBottom = new StringBuilder(1000);
		StringBuilder worstDailyTop = new StringBuilder(1000);
		StringBuilder worstDailyBottom = new StringBuilder(1000);
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
		StringBuilder sbCategoryReport = new StringBuilder(1000);
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

		differentialCSVPW.flush();
		differentialCSVPW.close();

		String mailMessage = getMailMessage(mailMessageFile);

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
		StringBuilder emailText = new StringBuilder(1000);
		if ((catOverallAverages.getTopValue(0) - 4.5) > (4.5 - catOverallAverages.getBottomValue(0))) {
			topReport(true, emailText, catOverallAverages, tabLookAhead, catETFList);
			bottomReport(false, emailText, catOverallAverages, tabLookAhead, catETFList);
		} else {
			bottomReport(true, emailText, catOverallAverages, tabLookAhead, catETFList);
			topReport(false, emailText, catOverallAverages, tabLookAhead, catETFList);
		}
		String xmlText = getReportFOXMLData(reportXMLFOFile);
		xmlText = xmlText.replace("<<<<MARKET DATE>>>>", currentMktDate);
		ArrayList<String> minidates = DateLine.forecast5DaysLine(currentMktDate, 30);
		xmlText = xmlText.replace("<<<miniblock0>>>", minidates.get(0));
		xmlText = xmlText.replace("<<<miniblock1>>>", minidates.get(1));
		xmlText = xmlText.replace("<<<miniblock2>>>", minidates.get(2));
		xmlText = xmlText.replace("<<<miniblock3>>>", minidates.get(3));
		xmlText = xmlText.replace("<<<miniblock4>>>", minidates.get(4));
		xmlText = xmlText.replace("<<<miniblock5>>>", minidates.get(5));

		xmlText = xmlText.replace("<<<<DATE BLOCK>>>", DateLine.forecastDateLine(currentMktDate, 30));

		if (etfExpectationsAbridged.length() < 2) {
			etfExpectationsAbridged.append("<fo:table-row background-color='lightgrey' >");
			etfExpectationsAbridged.append("<fo:table-cell><fo:block>No Data To Report</fo:block></fo:table-cell>");
			etfExpectationsAbridged.append("</fo:table-row>");
		}
		xmlText = xmlText.replace("<<<<WEEKLY RECOMMENDATION REQUIREMENT>>>>",
				"An ETF must have a 5-day period average of less than " + sellIndicatorLimit + "% for a sell signal"
						+ " or greater than " + buyIndicatorLimit + "% for a buy signal.");
		xmlText = xmlText.replace("<<<<ABRIDGED REPORT>>>>", etfExpectationsAbridged.toString());
		xmlText = xmlText.replace("<<<<CELL DATA>>>>", etfExpectations.toString());
		xmlText = xmlText.replace("<<<<BEST DAILY ETFS>>>>", bestDailyTop.toString() + bestDailyBottom.toString());
		xmlText = xmlText.replace("<<<<WORST DAILY ETFS>>>>", worstDailyTop.toString() + worstDailyBottom.toString());
		xmlText = xmlText.replace("<<<<CATEGORY DATA>>>>", sbCategoryReport.toString());
		xmlText = xmlText.replace("<<<<COPYRIGHT YEAR>>>>", currentMktDate.substring(0, 4));
		connRemote = getDatabaseConnection.makeMcVerryReportConnection();
		String historyForPDF = doHistory(connRemote, datesApart, currentMktDate);
		xmlText = xmlText.replace("<<<<HISTORY PDF REPORT>>>>", historyForPDF);
		StringBuilder catETFsb = new StringBuilder(1000);
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
			StringBuilder htmlMsg = new StringBuilder("<html><body>");
			if (mailMessage.length() > 2)
				htmlMsg.append("<h4>" + mailMessage.toString() + "</h4></p>");

			htmlMsg.append("<h2>DEBUG MODE ETF Expectations for " + currentMktDate + "</h2>");
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
			attachment4.setPath(estimatedPriceChangeCSVFile.getPath());
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
				StringBuilder htmlMsg = new StringBuilder("<html><body>");
				if (type.startsWith("Compli")) {
					htmlMsg.append("<h2>ETF Expectations for " + currentMktDate + "</h2>");
				} else {
					htmlMsg.append("<h2>ETF Expectations for " + currentMktDate + "</h2>");
				}

				if (mailMessage.length() > 2)
					htmlMsg.append("<h4>" + mailMessage.toString() + "</h4></p>");

				if (emailText.length() < 10) {
					htmlMsg.append("<h5>Since there are no significant data factors, there will be no "
							+ "quick sector analysis this evening. </h5>");

				} else {
					htmlMsg.append("<h3>Here is a quick analysis of the sectors. </h3>");
					htmlMsg.append(emailText.toString());
				}

				htmlMsg.append("<p>Your subscription count is now at " + cnt);
				if (cnt < 4)
					htmlMsg.append(".  <h5>Subscription rates went up except for my current subscribers."
							+ "  If you use the same subscription plan and renew within one week,"
							+ " I will refund you the difference between the new and your old rate. </h5> ");

				htmlMsg.append("</body></html>");
				eml.setHtmlMsg(htmlMsg.toString());
				EmailAttachment attachment = new EmailAttachment();
				attachment.setPath(pdfBothReportName);
				attachment.setDisposition(EmailAttachment.ATTACHMENT);
				eml.attach(attachment);
				if (type.endsWith("Daily")) {
					EmailAttachment attachment2 = new EmailAttachment();
					attachment2.setPath(estimatedPriceChangeCSVFile.getPath());
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

		estimatedPriceChangeCSVPW.close();

		UpdateResults.updateResultsTable(conn, resultsForDBFile, debugging);

	}

	public static String getReportFOXMLData(File reportXMLFOFile) throws IOException {
		FileReader fr = new FileReader(reportXMLFOFile);
		BufferedReader br = new BufferedReader(fr);
		String in = "";
		StringBuilder fsb = new StringBuilder(1000);
		while ((in = br.readLine()) != null) {
			fsb.append(in + "\n");
		}
		br.close();
		return fsb.toString();
	}

	public static String getMailMessage(File mailMessageFile) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(mailMessageFile));

		StringBuilder mailMessage = new StringBuilder(1000);

		String in = "";
		while ((in = br.readLine()) != null) {
			mailMessage.append(in + "\n");
		}

		if (mailMessage.toString().toLowerCase().startsWith("no message"))
			mailMessage = new StringBuilder();

		br.close();

		FileWriter fw = new FileWriter(mailMessageFile);
		fw.write("no message tonight");
		fw.flush();
		fw.close();
		return mailMessage.toString();
	}

	static public void topReport(boolean starter, StringBuilder emailText, TopAndBottomList catOverallAverages,
			TopAndBottomList tabLookAhead, TreeMap<String, StringBuilder> catETFList) {

		if (starter == false)
			emailText.append("<p>");

		// first do overall; based on catOverallAverages
		if (catOverallAverages.getTopValue(0) > 5.5) {
			boolean contrast = ThreadLocalRandom.current().nextBoolean();
			if (starter)
				emailText.append("The ");
			else
				emailText.append((contrast ? "In contrast" : "On the Bullish side") + ", the ");

			emailText.append(wordsCapitalized(catOverallAverages.getTopDescription()[0]) + " sector has a positive "
					+ " forecast for the next 30 trading days, with an average score of "
					+ df.format(catOverallAverages.getTopValue(0)) + ". ");
		}

		// now do short term period best
		if (tabLookAhead.getTopValue(0) > 4.5) {
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
					+ besties[1] + " days ");

			boolean strengthorAverage = ThreadLocalRandom.current().nextBoolean();
			emailText.append("with a computed " + (strengthorAverage ? "strength" : "average") + " of "
					+ df.format(tabLookAhead.getTopValue(0)) + ". ");

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
			emailText.append(" The estimated " + (bestOrTop ? "best" : "top") + " ETF in that "
					+ (bestOrTop ? "sector" : "category") + " for that period is $"
					+ topsym.getTopDescription()[0].toUpperCase() + ". ");

			int ii = 1;
			while (tabLookAhead.getTopDescription()[ii].startsWith(besties[0]) == true
					| tabLookAhead.getTopDescription()[ii]
							.startsWith(catOverallAverages.getTopDescription()[0]) == true) {
				ii++;
				if (ii >= tabLookAhead.size)
					return;
			}
			if (tabLookAhead.getTopValue(ii) > 4.5) {
				besties = tabLookAhead.getTopDescription()[ii].split("<");
				periods = besties[1].split(" ");
				int p11 = Integer.parseInt(periods[0]);
				int p21 = Integer.parseInt(periods[2]);
				topsym = new TopAndBottomList();
				bestOrTop = ThreadLocalRandom.current().nextBoolean();
				emailText.append(" Next is followed by the " + wordsCapitalized(besties[0]) + " sector with a "
						+ (bestOrTop ? "good" : "better than average") + " forecast");
				if (p11 == p1 & p21 == p2)
					emailText.append(" for the same time frame ");
				else
					emailText.append(" in " + besties[1] + " days ");
				strengthorAverage = ThreadLocalRandom.current().nextBoolean();
				emailText.append(" with a computed " + (strengthorAverage ? "strength" : "average") + " of "
						+ df.format(tabLookAhead.getTopValue(1)) + ". ");
				catSyms = catETFList.get(besties[0]).toString().split(" ");
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
						" The estimated " + (bestOrTop ? "best" : "top") + " ETF in this sector for the period is $"
								+ topsym.getTopDescription()[0].toUpperCase() + ". ");
			}
		}

	}

	static void bottomReport(boolean starter, StringBuilder emailText, TopAndBottomList catOverallAverages,
			TopAndBottomList tabLookAhead, TreeMap<String, StringBuilder> catETFList) {

		if (starter == false)
			emailText.append("<p>");

		// first do overall; based on catOverallAverages
		if (catOverallAverages.getBottomValue(0) < 3.5) {
			if (starter)
				emailText.append("The ");
			else
				emailText.append("In contrast, the ");
			emailText.append(wordsCapitalized(catOverallAverages.getBottomDescription()[0]) + " sector has a bearish "
					+ " forecast for the next 30 trading days, with an average score of "
					+ df.format(catOverallAverages.getBottomValue(0)) + ". ");
		}

		// now do short term period worst
		if (tabLookAhead.getBottomValue(0) < 4.5) {
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
			boolean strengthOrAverage = ThreadLocalRandom.current().nextBoolean();
			emailText.append("the computed " + (strengthOrAverage ? "strength" : "average") + " is "
					+ df.format(tabLookAhead.getBottomValue(0)) + ".");

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

			int ii = 1;
			while (tabLookAhead.getBottomDescription()[ii].startsWith(besties[0]) == true
					| tabLookAhead.getBottomDescription()[ii]
							.startsWith(catOverallAverages.getBottomDescription()[0]) == true) {
				ii++;
				if (ii >= tabLookAhead.size)
					return;
			}
			if (tabLookAhead.getBottomValue(1) < 4.5) {
				besties = tabLookAhead.getBottomDescription()[ii].split("<");
				periods = besties[1].split(" ");
				int p11 = Integer.parseInt(periods[0]);
				int p21 = Integer.parseInt(periods[2]);
				emailText.append(
						" Coming in second with a weak prospect is the " + wordsCapitalized(besties[0]) + " sector ");
				if (p11 == p1 & p21 == p2)
					emailText.append("for the same period ");
				else
					emailText.append("in " + besties[1] + " days");
				strengthOrAverage = ThreadLocalRandom.current().nextBoolean();
				emailText.append(" with a computed " + (strengthOrAverage ? "strength" : "average") + " of "
						+ df.format(tabLookAhead.getBottomValue(1)) + ".");
				topsym = new TopAndBottomList();
				catSyms = catETFList.get(besties[0]).toString().split(" ");
				for (String csym : catSyms) {
					double avg[] = symbolDailyAverages.get(csym);
					if (avg == null)
						continue;
					for (int i = p1; i <= p2; i++) {
						topsym.setBottom(avg[i], csym);
					}
				}
				worseOrLowest = ThreadLocalRandom.current().nextBoolean();
				emailText.append(" The models expect the ETF $" + topsym.getBottomDescription()[0].toUpperCase()
						+ " to finish the " + (worseOrLowest ? "worst" : "lowest") + " in this group. ");
			}
		}

	}

	static String wordsCapitalized(String in) {
		StringBuilder sb = new StringBuilder(1000);
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

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	DeltaBands priceBands;

	public static String currentMktDate;
	TreeMap<String, Object> myParms;

	TreeMap<String, Double> theBadness;
	boolean whichHalf = false;

	Semaphore threadSemaphore;

	static TreeMap<String, Averager> threadRunAverages = new TreeMap<>();

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
			pwWebPage.print(" Bullish:");
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

		StringBuilder histPDF = new StringBuilder(1000);

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

	public static void updateResultsTable(Connection conn, File resultsForDBFile2, boolean debugging) throws Exception {

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
					+ " on duplicate key update guess = (values(guess) + ?) / 2 ");
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
			try (BufferedReader br = new BufferedReader(new FileReader("holidays.txt"))) {
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
			}
		} catch (Exception e) {
			e.printStackTrace();

			System.exit(0);
		}

		return false;

	}

}
