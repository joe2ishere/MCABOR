package correlation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import bands.DeltaBands;
import util.Averager;
import util.getDatabaseConnection;
import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class CorrelationEstimatorTesting extends CorrelationEstimator {
	public CorrelationEstimatorTesting(Connection conn) {
		super(conn);
		// TODO Auto-generated constructor stub
	}

	public static void loadGSDSTable(Connection conn) throws Exception {
		PreparedStatement selectSymbols = conn
				.prepareStatement("select distinct  symbol  from macd_correlation  order by symbol");
		ResultSet rsSymbols = selectSymbols.executeQuery();
		while (rsSymbols.next()) {
			String sym = rsSymbols.getString(1);

			GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
			gsds.put(sym, gsd);
		}
	}

	public static void main(String args[]) throws Exception {

		LogManager.getLogManager().reset();

		functionDayAverager = CorrelationFunctionPerformance.loadFromFile();

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

		// PrintStream averageOutPW = System.out;

		ArrayList<String> datesApart = null;
		TreeMap<String, String> day5 = new TreeMap<>();
		TreeMap<String, String> day10 = new TreeMap<>();
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

//				doHistory(DriverManager.getConnection(
//						"jdbc:mysql://mcverryreport.com/mcverr5_reports?user=mcverr5_sqlDwnld&password=y(Y}^mOZ+bOA"),
//						datesApart, currentMktDate);

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
				catETFList.put(cat, new StringBuffer());
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

		while (rsSelectDays.next()) {
			int day = rsSelectDays.getInt(1);
			daysOutList.add(day);
			System.out.print(";D.O. " + day);
			dailyHighlyFavored.put(day, new Favored("", -1.));
			dailyLeastFavored.put(day, new Favored("", 99.));

		}
		System.out.println(";Avg. 1-5; Avg. 6-10; Avg. 11-15;Avg. 16-20; Avg. 21-25; Avg. 26-30;Avg.");

		ArrayList<CorrelationEstimator> estimators = new ArrayList<>();
		// TODO set estimators

		estimators.add(new ADXCorrelationEstimator(conn));
		estimators.add(new ATRCorrelationEstimator(conn));
//		estimators.add(new BBCorrelationEstimator(conn));
		estimators.add(new DMICorrelationEstimator(conn));
		estimators.add(new MACDCorrelationEstimator(conn));
		estimators.add(new TSFCorrelationEstimator(conn));
		estimators.add(new CCICorrelationEstimator(conn));

		debugging = true;
		ArrayList<String> categoryDoneforDebugging = new ArrayList<>();

		for (String sym : gsds.keySet()) {

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

//				if (sym.startsWith("sco") == false)
//					continue;
				}
			}
			System.out.print(sym);

			Averager avverage = new Averager();
			TreeMap<Integer, Averager> avgForDaysOut = new TreeMap<>();

			TreeMap<Integer, Averager> catAvg = null;
			if (symCats.containsKey(sym))
				catAvg = catAverages.get(symCats.get(sym));

			TreeMap<String, Double> theBadness = new TreeMap<>();

			for (int daysOut : daysOutList) {
//				if (daysOut != 11)
//					continue;
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

//			for (int daysOut : daysOutList) {
//				String badnessFileName = "c:/users/joe/correlationARFF/bad/" + sym + "_" + daysOut
//						+ "_allbad_correlation.arff";
//				StringBuffer sb = new StringBuffer();
//				String in = "";
//				BufferedReader brBad = new BufferedReader(new FileReader(badnessFileName));
//				while ((in = brBad.readLine()) != null) {
//					sb.append(in + "\n");
//				}
//				brBad.close();
//				sb.append(theBadness.get("adx;" + daysOut) + ",");
//				sb.append(theBadness.get("atr;" + daysOut) + ",");
////				sb.append(theBadness.get("bb;" + daysOut) + ",");
//				sb.append(theBadness.get("dmi;" + daysOut) + ",");
//				sb.append(theBadness.get("macd;" + daysOut) + ",");
//				sb.append(theBadness.get("tsf;" + daysOut) + ",");
//				sb.append("?");
//				Instances instances = new Instances(new StringReader(sb.toString()));
//				int adxClassPos = instances.numAttributes() - 1;
//				instances.setClassIndex(adxClassPos);
//				LinearRegression classifier = new LinearRegression();
//				classifier.buildClassifier(instances);
//				double got = classifier.classifyInstance(instances.get(instances.size() - 1));
//				avgForDaysOut.get(daysOut).add(got, daysOut / 5);
//
//			}

			double savg[] = new double[31];
			for (Integer key : avgForDaysOut.keySet()) {
				double theAvg = avgForDaysOut.get(key).get();
				System.out.print(";" + df.format(theAvg));
				savg[key] = theAvg;
				if (catAvg != null) {
					Averager avg = catAvg.get(key);
					if (avg == null) {
						avg = new Averager();
						catAvg.put(key, avg);
					}
					avg.add(theAvg);
				}
			}

			symbolDailyAverages.put(sym, savg);

			double averages[] = new double[30];
			for (Integer daysOut : avgForDaysOut.keySet()) {
				averages[daysOut - 1] = avgForDaysOut.get(daysOut).get();
			}
			for (int daysOut = 4; daysOut <= 29; daysOut += 5) {
				Averager outAvg = new Averager();
				for (int daysBack = 0; daysBack <= 4; daysBack++) {
					outAvg.add(averages[daysOut - (4 - daysBack)], (daysBack + 1) * (daysBack + 1));
				}

				System.out.print(";" + df.format(outAvg.get()));
				if (outAvg.get() > buyIndicatorLimit) {

					{
						if (daysOut == 4) {
							if (doneList.contains(sym) == false) {
								B5.add(sym);
								day5.put(sym, "Buy");
							}
							doneList.add(sym);

						} else if (daysOut == 9 & B5.contains(sym) == false & S5.contains(sym) == false) {
							if (doneList.contains(sym) == false & B5.contains(sym) == false) {
								B10.add(sym);
								day10.put(sym, "Buy");
							}
							doneList.add(sym);

						}
					}
				} else if (outAvg.get() < sellIndicatorLimit) {

					{
						if (daysOut == 4) {
							if (doneList.contains(sym) == false) {
								S5.add(sym);
								day5.put(sym, "Sell");
							}
							doneList.add(sym);

						} else if (daysOut == 9 & B5.contains(sym) == false & S5.contains(sym) == false) {
							if (doneList.contains(sym) == false & S5.contains(sym) == false) {
								S10.add(sym);
								day10.put(sym, "Sell");
							}
							doneList.add(sym);

						}
					}

				}
			}

			System.out.println(";" + df.format(avverage.get()/* + ";" + overAllAverageStrength.get() */));

		}

	}

	public void setWork(String sym, int daysOut, Averager average, DeltaBands priceBands,
			TreeMap<Integer, Averager> smiaverageForDaysOut, TreeMap<String, Double> theBadness) throws Exception {

		this.sym = sym;
		this.daysOut = daysOut;

		myParms = new TreeMap<>();
		functionDaysDiffMap = new TreeMap<>();
		doubleBacks = new TreeMap<>();
		dates = new TreeMap<>();

		this.averager = average;
		this.priceBands = priceBands;

		this.avgForDaysOut = smiaverageForDaysOut;

		psSelect.setString(1, sym);
		psSelect.setInt(2, daysOut);

		this.theBadness = theBadness;

	}

	public String buildInstances() {

		StringWriter sw = null;
		PrintWriter pw = null;
		sw = new StringWriter();
		pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_" + function + "_correlation");
		pw.println("@RELATION " + sym);
		int pos = 50;
		GetETFDataUsingSQL gsd = gsds.get(sym);
		startDate = gsd.inDate[50];
		try {
			buildHeaders(pw);
		} catch (Exception e1) {

			e1.printStackTrace();
			System.exit(0);
			return null;
		}
		pw.println("@ATTRIBUTE class NUMERIC");

		pw.println("@DATA");
		pw.flush();
		if (myParms.size() == 0)
			return null;
		int arraypos[] = new int[myParms.size()];
		pos = 0;

		arraypos[pos] = 0;

		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		eemindexLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut - 1;) {

			String posDate = gsd.inDate[iday];
			pos = 0;
			for (String key : myParms.keySet()) {
				String sdate = dates.get(key)[arraypos[pos]];
				int dcomp = posDate.compareTo(sdate);
				if (dcomp < 0) {
					iday++;
					continue eemindexLoop;
				}
				if (dcomp > 0) {
					arraypos[pos]++;
					continue eemindexLoop;
				}
				pos++;
			}
			pos = 0;
			for (String key : myParms.keySet()) {
				int giDay = iday - functionDaysDiffMap.get(key);
				int siDay = arraypos[pos] - functionDaysDiffMap.get(key);

				for (int i = 0; i < (functionDaysDiffMap.get(key) + daysOut); i++) {
					String gtest = gsd.inDate[giDay + i];
					String stest = dates.get(key)[siDay + i];
					if (gtest.compareTo(stest) != 0) {
						iday++;
						continue eemindexLoop;
					}
				}
				pos++;
			}

			Date now;
			try {
				now = sdf.parse(posDate);
			} catch (ParseException e) {

				e.printStackTrace();
				System.exit(0);
				return null;
			}

			Calendar cdMove = Calendar.getInstance();
			cdMove.setTime(now);
			int idt = 1;
			int daysOutCalc = 0;
			while (true) {
				cdMove.add(Calendar.DAY_OF_MONTH, +1);
				if (cdMove.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY)
					continue;
				if (cdMove.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
					continue;
				daysOutCalc++;
				idt++;
				if (idt > daysOut)
					break;
			}
			String endDate = gsd.inDate[iday + daysOut];
			if (daysOutCalc != daysOut)
				System.out.println(posDate + " here " + endDate + " by " + daysOutCalc + " not " + daysOut);
			// pw.print(gsd.inDate[iday] + ",");
			printAttributeData(iday, daysOutCalc, pw, functionDaysDiffMap, doubleBacks, arraypos, gsd.inClose,
					priceBands);
			iday++;
			for (pos = 0; pos < arraypos.length; pos++) {
				arraypos[pos]++;
			}

		}
		pos = 0;
		for (String key : myParms.keySet()) {
			Object myParm = myParms.get(key);
			int functionDaysDiff = functionDaysDiffMap.get(key);
			int start = dates.get(key).length - 1;
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			getAttributeText(pw, myParm, functionDaysDiff, start, doubleBack.intValue());

			arraypos[pos]++;
			pos++;

		}

		pw.println("?");
		pw.flush();
		return sw.toString();
	}

	@Override
	public void run() {
		Instant start = Instant.now();
		String $instances = buildInstances();

		if ($instances == null)
			return;
		Instances instances;

		try {
			instances = new Instances(new StringReader($instances));
			instances.setClassIndex(instances.numAttributes() - 1);

			double got;

			got = drun(instances);
			setResultsForDB(got);

			if (Double.isNaN(got)) {
				System.err.println("found a bad one ");
				return;
			}

			theBadness.put(function + ";" + daysOut, got);
			synchronized (instances) {
				Averager thisDaysAverage = avgForDaysOut.get(daysOut);
				if (thisDaysAverage == null) {
					thisDaysAverage = new Averager();
					avgForDaysOut.put(daysOut, thisDaysAverage);
				}
				// System.out.println("\n"+this.getClass().getSimpleName() + ";" + got);
				thisDaysAverage.add(got);
				double funcAvg = .5;
				ArrayList<Averager> funcAverages = functionDayAverager.get(this.function);
				if (funcAverages != null) {
					Averager funcDayAverage = funcAverages.get(daysOut);
					if (funcDayAverage != null)
						funcAvg = funcDayAverage.get();
				}
				averager.add(got, funcAvg);

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
			avg.add(1. * Duration.between(start, finish).toMillis());
		}

	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

	public double drun(Instances instances) throws Exception {
		Classifier classifier = new IBk();
		String args[] = { "-I" };
		((IBk) classifier).setOptions(args);

//		Classifier classifier = new LinearRegression();

		classifier.buildClassifier(instances);
		return classifier.classifyInstance(instances.get(instances.size() - 1));

	}

//	public double prun(Instances instances) throws Exception {
//		Classifier classifier = new IBk();
//		String args[] = { "-I" };
//		((IBk) classifier).setOptions(args);
//
//		classifier.buildClassifier(instances);
//		return classifier.classifyInstance(instances.get(instances.size() - 1));
//
//	}

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

}
