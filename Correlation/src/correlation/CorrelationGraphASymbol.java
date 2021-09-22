package correlation;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.TreeMap;
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

		TreeMap<Integer, Averager> performanceAverageForDaysOut = new TreeMap<>();

		ArrayList<Double> adxAverages = new ArrayList<>(30);
		ArrayList<Double> atrAverages = new ArrayList<>(30);
//		ArrayList<Double> bbAverages = new ArrayList<>(30);
		ArrayList<Double> dmiAverages = new ArrayList<>(30);
		ArrayList<Double> macdAverages = new ArrayList<>(30);
		ArrayList<Double> tsfAverages = new ArrayList<>(30);
		for (int i = 0; i < 30; i++) {
			adxAverages.add(0.);
			atrAverages.add(0.);
			// bbAverages.add(0.);
			dmiAverages.add(0.);
			macdAverages.add(0.);
			tsfAverages.add(0.);
		}

		System.out.println(sym);

		TreeMap<Integer, Averager> adxaverageForDaysOut = new TreeMap<>();
		TreeMap<Integer, Averager> atraverageForDaysOut = new TreeMap<>();
//		TreeMap<Integer, Averager> bbaverageForDaysOut = new TreeMap<>();
		TreeMap<Integer, Averager> dmiaverageForDaysOut = new TreeMap<>();
		TreeMap<Integer, Averager> macdaverageForDaysOut = new TreeMap<>();
		TreeMap<Integer, Averager> tsfaverageForDaysOut = new TreeMap<>();
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

			performanceAverageForDaysOut.put(daysOut, new Averager());

			ArrayList<Thread> threads = new ArrayList<>();

			threads.add(new Thread(() -> {
				try {
					adxaverageForDaysOut.put(daysOut, new Averager());
					ADXCorrelationEstimator adx = new ADXCorrelationEstimator(conn);
					adx.setWork(sym, daysOut, priceBands, adxaverageForDaysOut, theBadness);
					adx.run();
					double got = adxaverageForDaysOut.get(daysOut).get();
					doAverages(adxAverages, got, daysOut, priceBands);
				} catch (Exception e1) {
					e1.printStackTrace();
					return;
				}
			}));

//			threads.add(new Thread(() -> {
//				try {
//					bbaverageForDaysOut.put(daysOut, new Averager());
//					BBCorrelationEstimator bb = new BBCorrelationEstimator(conn);
//					bb.setWork(sym, daysOut, avg, priceBands, bbaverageForDaysOut, theBadness);
//					bb.run();
//					double got = bbaverageForDaysOut.get(daysOut).get();
//					doAverages(bbAverages, got, daysOut, priceBands);
//				} catch (Exception e1) {
//					e1.printStackTrace();
//					return;
//				}
//			}));
			threads.add(new Thread(() -> {
				try {
					dmiaverageForDaysOut.put(daysOut, new Averager());
					DMICorrelationEstimator dmi = new DMICorrelationEstimator(conn);
					dmi.setWork(sym, daysOut, priceBands, dmiaverageForDaysOut, theBadness);
					dmi.run();
					double got = dmiaverageForDaysOut.get(daysOut).get();
					doAverages(dmiAverages, got, daysOut, priceBands);
				} catch (Exception e1) {
					e1.printStackTrace();
					return;
				}
			}));

			threads.add(new Thread(() -> {
				try {
					macdaverageForDaysOut.put(daysOut, new Averager());
					MACDCorrelationEstimator macd;
					macd = new MACDCorrelationEstimator(conn);
					macd.setWork(sym, daysOut, priceBands, macdaverageForDaysOut, theBadness);
					macd.run();
					double got = macdaverageForDaysOut.get(daysOut).get();
					doAverages(macdAverages, got, daysOut, priceBands);
				} catch (Exception e1) {
					e1.printStackTrace();
					return;
				}
			}));

			threads.add(new Thread(() -> {
				try {
					tsfaverageForDaysOut.put(daysOut, new Averager());
					TSFCorrelationEstimator tsf = new TSFCorrelationEstimator(conn);
					tsf.setWork(sym, daysOut, priceBands, tsfaverageForDaysOut, theBadness);
					tsf.run();
					double got = tsfaverageForDaysOut.get(daysOut).get();
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
			StringBuffer sb = new StringBuffer();
			String in = "";
			BufferedReader brBad = new BufferedReader(new FileReader(badnessFileName));
			while ((in = brBad.readLine()) != null) {
				sb.append(in + "\n");
			}
			brBad.close();
//			sb.append(theBadness.get("adx;" + daysOut) + ",");
//			sb.append(theBadness.get("atr;" + daysOut) + ",");
//			sb.append(theBadness.get("bb;" + daysOut) + ",");
			sb.append(theBadness.get("dmi;" + daysOut) + ",");
			sb.append(theBadness.get("macd;" + daysOut) + ",");
			sb.append(theBadness.get("tsf;" + daysOut) + ",");
			sb.append("?");
			Instances instances = new Instances(new StringReader(sb.toString()));
			PrintWriter pwCGAS = new PrintWriter("c:/users/joe/correlationARFF/" + sym + "CGAS.arff");
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
			thisavg.add(adxAverages.get(i));
			thisavg.add(atrAverages.get(i));
			thisavg.add(dmiAverages.get(i));
			thisavg.add(macdAverages.get(i));
			thisavg.add(tsfAverages.get(i));
			thisavg.add(errorDaysOut.get(i));
			allAverages.add(thisavg.get());
			// oneLine.add(100*(1.);
		}

		chart.addSeries("ADX", adxAverages).setMarker(SeriesMarkers.SQUARE).setLineWidth(1);
		chart.addSeries("ATR", atrAverages).setMarker(SeriesMarkers.TRIANGLE_DOWN).setLineWidth(1);
		chart.addSeries("DMI", dmiAverages).setMarker(SeriesMarkers.CROSS).setLineWidth(1);
		chart.addSeries("MACD", macdAverages).setMarker(SeriesMarkers.DIAMOND).setLineWidth(1);
		chart.addSeries("TSF", tsfAverages).setMarker(SeriesMarkers.TRAPEZOID).setLineWidth(1);
		chart.addSeries("Err Correction", tsfAverages).setMarker(SeriesMarkers.OVAL).setLineWidth(2);

// chart.addSeries("1", oneLine).setLineWidth(1).setLineColor(Color.RED);
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
