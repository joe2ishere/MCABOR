package correlation;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.logging.LogManager;

import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.BitmapEncoder.BitmapFormat;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.style.markers.SeriesMarkers;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import bands.DeltaBands;
import util.Averager;
import util.getDatabaseConnection;
import utils.StandardDeviation;
import weka.classifiers.functions.LinearRegression;
import weka.core.Instances;

public class CorrelationGraphASymbol {

	static DecimalFormat df = new DecimalFormat("#.##");

	static String startDate = null;

	// static IBk classifierIBk = new IBk();

	public static void main(String[] args) throws Exception {

		Scanner in = new Scanner(System.in);
		String sym;
		while (true) {
			System.out.print("\nSymbol: ");
			sym = in.nextLine().trim();

			if (sym == null | sym.length() < 1) {
				in.close();
				return;
			}
			break;
		}
		run(sym);
	}

	public static void run(String sym) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();
		LogManager.getLogManager().reset();
		final PreparedStatement stGetDate = conn
				.prepareStatement("select distinct txn_date from etfprices order by txn_date desc limit 1");
		ResultSet rsDate = stGetDate.executeQuery();
		rsDate.next();
		CorrelationEstimator.currentMktDate = rsDate.getString(1);

		CorrelationEstimator.loadGSDSTable(conn);
		CorrelationEstimator.functionDayAverager = CorrelationFunctionPerformance.loadFromFile();

		final PreparedStatement smiSelectDays = conn
				.prepareStatement("select distinct " + " toCloseDays  from dmi_correlation" + " order by toCloseDays");
		ResultSet rsSelectDays = smiSelectDays.executeQuery();
		ArrayList<Integer> daysOutList = new ArrayList<Integer>();

		while (rsSelectDays.next()) {
			daysOutList.add(rsSelectDays.getInt(1));

		}

		TreeMap<Integer, StandardDeviation> performanceAverageForDaysOut = new TreeMap<>();

		ArrayList<Double> dmiAverages = new ArrayList<>(30);
		ArrayList<Double> maavgAverages = new ArrayList<>(30);
		ArrayList<Double> macdAverages = new ArrayList<>(30);
		ArrayList<Double> maliAverages = new ArrayList<>(30);
		ArrayList<Double> smiAverages = new ArrayList<>(30);
		ArrayList<Double> tsfAverages = new ArrayList<>(30);
		for (int i = 0; i < 30; i++) {
			dmiAverages.add(0.);
			maavgAverages.add(0.);
			macdAverages.add(0.);
			maliAverages.add(0.);
			smiAverages.add(0.);
			tsfAverages.add(0.);
		}

		System.out.println(sym);

		TreeMap<Integer, StandardDeviation> dmiaverageForDaysOut = new TreeMap<>();
		TreeMap<Integer, StandardDeviation> maavgaverageForDaysOut = new TreeMap<>();
		TreeMap<Integer, StandardDeviation> macdaverageForDaysOut = new TreeMap<>();
		TreeMap<Integer, StandardDeviation> maliaverageForDaysOut = new TreeMap<>();
		TreeMap<Integer, StandardDeviation> smiaverageForDaysOut = new TreeMap<>();
		TreeMap<Integer, StandardDeviation> tsfaverageForDaysOut = new TreeMap<>();
		TreeMap<Integer, Double> errorDaysOut = new TreeMap<>();
		TreeMap<String, Double> theBadness = new TreeMap<>();

		GetETFDataUsingSQL gsd = CorrelationEstimator.gsds.get(sym);
		if (gsd == null) {
			System.out.println("can't find " + sym);
			return;
		}
		for (int daysOut : daysOutList) {
//			if (daysOut != 11)
//				continue;
			DeltaBands priceBands = new DeltaBands(gsd.inClose, daysOut);

			performanceAverageForDaysOut.put(daysOut, new StandardDeviation());

			ArrayList<Thread> threads = new ArrayList<>();
			Semaphore threadSemaphore = new Semaphore(10);

			threads.add(new Thread(() -> {
				try {
					dmiaverageForDaysOut.put(daysOut, new StandardDeviation());
					DMICorrelationEstimator dmi = new DMICorrelationEstimator(conn, true);
					dmi.setWork(sym, daysOut, priceBands, dmiaverageForDaysOut, theBadness, false, threadSemaphore);
					dmi.run();
					double got = dmiaverageForDaysOut.get(daysOut).getMean();
					doAverages(dmiAverages, got, daysOut, priceBands);
				} catch (Exception e1) {
					e1.printStackTrace();
					return;
				}
			}));

			threads.add(new Thread(() -> {
				try {
					maavgaverageForDaysOut.put(daysOut, new StandardDeviation());
					MAAveragerCorrelationEstimator maavg = new MAAveragerCorrelationEstimator(conn);
					maavg.setWork(sym, daysOut, priceBands, maavgaverageForDaysOut, theBadness, false, threadSemaphore);
					maavg.run();
					double got = maavgaverageForDaysOut.get(daysOut).getMean();
					doAverages(maavgAverages, got, daysOut, priceBands);
				} catch (Exception e1) {
					e1.printStackTrace();
					return;
				}
			}));

			threads.add(new Thread(() -> {
				try {
					macdaverageForDaysOut.put(daysOut, new StandardDeviation());
					MACDCorrelationEstimator macd;
					macd = new MACDCorrelationEstimator(conn, true);
					macd.setWork(sym, daysOut, priceBands, macdaverageForDaysOut, theBadness, false, threadSemaphore);
					macd.run();
					double got = macdaverageForDaysOut.get(daysOut).getMean();
					doAverages(macdAverages, got, daysOut, priceBands);
				} catch (Exception e1) {
					e1.printStackTrace();
					return;
				}
			}));

			threads.add(new Thread(() -> {
				try {
					maliaverageForDaysOut.put(daysOut, new StandardDeviation());
					MALinesCorrelationEstimator mali = new MALinesCorrelationEstimator(conn);
					mali.setWork(sym, daysOut, priceBands, maliaverageForDaysOut, theBadness, false, threadSemaphore);
					mali.run();
					double got = maliaverageForDaysOut.get(daysOut).getMean();
					doAverages(maliAverages, got, daysOut, priceBands);
				} catch (Exception e1) {
					e1.printStackTrace();
					return;
				}
			}));

			threads.add(new Thread(() -> {
				try {
					smiaverageForDaysOut.put(daysOut, new StandardDeviation());
					SMICorrelationEstimator smi = new SMICorrelationEstimator(conn, true);
					smi.setWork(sym, daysOut, priceBands, smiaverageForDaysOut, theBadness, false, threadSemaphore);
					smi.run();
					double got = smiaverageForDaysOut.get(daysOut).getMean();
					doAverages(smiAverages, got, daysOut, priceBands);
				} catch (Exception e1) {
					e1.printStackTrace();
					return;
				}
			}));

			threads.add(new Thread(() -> {
				try {
					tsfaverageForDaysOut.put(daysOut, new StandardDeviation());
					TSFCorrelationEstimator tsf = new TSFCorrelationEstimator(conn, true);
					tsf.setWork(sym, daysOut, priceBands, tsfaverageForDaysOut, theBadness, false, threadSemaphore);
					tsf.run();
					double got = tsfaverageForDaysOut.get(daysOut).getMean();
					doAverages(tsfAverages, got, daysOut, priceBands);
				} catch (Exception e1) {
					e1.printStackTrace();
					return;
				}
			}));

			for (Thread thrd : threads) {
				thrd.start();
			}
			for (Thread thrd : threads) {
				thrd.join();
			}

			String badnessFileName = "c:/users/joe/correlationARFF/bad/" + sym + "_" + daysOut
					+ "_allbad_correlation.arff";
			StringBuffer sb = new StringBuffer(1000);
			String in = "";
			BufferedReader brBad = new BufferedReader(new FileReader(badnessFileName));
			while ((in = brBad.readLine()) != null) {
				sb.append(in + "\n");
			}
			brBad.close();

			sb.append(theBadness.get("dmi;" + daysOut) + ",");
			sb.append(theBadness.get("maAvg;" + daysOut) + ",");
			sb.append(theBadness.get("macd;" + daysOut) + ",");
			sb.append(theBadness.get("mali;" + daysOut) + ",");
			sb.append(theBadness.get("smi;" + daysOut) + ",");
			sb.append(theBadness.get("tsf;" + daysOut) + ",");
			sb.append("?");
			Instances instances = new Instances(new StringReader(sb.toString()));
			StringWriter sw = new StringWriter();
			PrintWriter pwCGAS = new PrintWriter(sw);
			pwCGAS.println(sb.toString());
			pwCGAS.flush();
			pwCGAS.close();
			int adxClassPos = instances.numAttributes() - 1;
			instances.setClassIndex(adxClassPos);
			LinearRegression classifier = new LinearRegression();
			classifier.buildClassifier(instances);
			double got = classifier.classifyInstance(instances.get(instances.size() - 1));
			if (got > 9)
				got = 9;
			errorDaysOut.put(daysOut - 1, 100 * (priceBands.getApproximiateValue(got)));

		}

		XYChartBuilder chartBuilder = new XYChartBuilder();
		chartBuilder.width(600);
		chartBuilder.height(600);

		XYChart chart = new XYChartBuilder().build();
		chart.setTitle("$" + sym.toUpperCase() + "     -     Estimated Price % Change Trend for Next 30 Days");
		chart.setXAxisTitle("Days Out             -             $" + sym.toUpperCase());
		chart.setYAxisTitle("Estimated Percent Change");

		ArrayList<Double> allAverages = new ArrayList<>();
		// ArrayList<Double> oneLine = new ArrayList<>();
		for (int i = 0; i < macdAverages.size(); i++) {
			Averager thisavg = new Averager();

			thisavg.add(dmiAverages.get(i));
			thisavg.add(macdAverages.get(i));
			thisavg.add(maavgAverages.get(i));
			thisavg.add(maliAverages.get(i));
			thisavg.add(tsfAverages.get(i));
			thisavg.add(errorDaysOut.get(i));
			allAverages.add(thisavg.get());
		}

		chart.addSeries("DMI", dmiAverages).setMarker(SeriesMarkers.CROSS).setLineWidth(1);
		chart.addSeries("MAAVG", maavgAverages).setMarker(SeriesMarkers.RECTANGLE).setLineWidth(1);
		chart.addSeries("MACD", macdAverages).setMarker(SeriesMarkers.DIAMOND).setLineWidth(1);
		chart.addSeries("MALI", maliAverages).setMarker(SeriesMarkers.SQUARE).setLineWidth(1);
		chart.addSeries("SMI", smiAverages).setMarker(SeriesMarkers.CIRCLE).setLineWidth(1);
		chart.addSeries("TSF", tsfAverages).setMarker(SeriesMarkers.TRAPEZOID).setLineWidth(1);
		chart.addSeries("Err Correction", tsfAverages).setMarker(SeriesMarkers.OVAL).setLineWidth(2);

		chart.addSeries("Average", allAverages).setMarker(SeriesMarkers.CIRCLE).setLineWidth(4)
				.setLineColor(Color.BLACK);

		new SwingWrapper<XYChart>(chart).displayChart().setTitle("Coorelated Symbols with IBk Ensemble and Average");
		BitmapEncoder.saveBitmap(chart, "c:/users/joe/correlationImages/" + sym + "_Correlated_Chart",
				BitmapFormat.PNG);

	}

	static void doAverages(ArrayList<Double> averages, Double got, int daysOut, DeltaBands priceBands)
			throws Exception {
		averages.set(daysOut - 1, 100 * (priceBands.getApproximiateValue(got)));

	}

}
