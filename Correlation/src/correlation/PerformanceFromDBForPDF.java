package correlation;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import util.Averager;
import util.getDatabaseConnection;

public class PerformanceFromDBForPDF {

	public static void main(String[] args) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cd = Calendar.getInstance();

		makeReport(sdf.format(cd.getTime()));
	}

	public static String makeReport(String currentMktDate) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();
		PreparedStatement psGetDates = conn
				.prepareStatement("select distinct mktDate from correlation30days order by mktDate desc limit 40 ");
		ArrayList<String> mktDates = new ArrayList<>();
		ResultSet rsMktDates = psGetDates.executeQuery();
		while (rsMktDates.next()) {
			mktDates.add(rsMktDates.getString(1));
		}

		PreparedStatement psGetGuesses = conn
				.prepareStatement("select symbol, guesses from correlation30days where mktDate=? ");

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		FileReader fr = new FileReader("xmlFilesForPDFReports/Report-Performance-FO.xml");
		StringBuffer sb = new StringBuffer();
		BufferedReader br = new BufferedReader(fr);
		String in = "";
		while ((in = br.readLine()) != null) {
			sb.append(in + "\n");
		}
		br.close();

		String allText = sb.toString();
		sb = new StringBuffer();
		TreeMap<Integer, Averager> allDaysAverage = new TreeMap<>();
		TreeMap<String, TreeMap<Integer, Averager>> allSheetsDaysAverage = new TreeMap<>();
		String rptDate = null;
		Averager avgRightWrong = new Averager();
		Averager avgBuyRightWrong = new Averager();
		Averager avgSellRightWrong = new Averager();
		Averager avgBuyPctChange = new Averager();
		Averager avgSellPctChange = new Averager();

		for (String mktDate : mktDates) {

			if (rptDate == null) {
				// there is no data for the last market date so get it and continue;
				rptDate = mktDate;
				continue;
			}

			TreeMap<Integer, Averager> sheetDaysAverage = new TreeMap<>();
			allSheetsDaysAverage.put(mktDate, sheetDaysAverage);

			Date sheetDt = sdf.parse(mktDate);
			Calendar sheetClndr = Calendar.getInstance();
			sheetClndr.setTime(sheetDt);
			psGetGuesses.setString(1, mktDate);

			Averager rightWrong = new Averager();
			Averager buyRightWrong = new Averager();
			Averager sellRightWrong = new Averager();
			Averager buyPctChange = new Averager();
			Averager sellPctChange = new Averager();

			ResultSet rsGetGuesses = psGetGuesses.executeQuery();

			nextGuess: while (rsGetGuesses.next()) {
				String symbol = rsGetGuesses.getString(1);
				GetETFDataUsingSQL gsd;
				try {
					gsd = GetETFDataUsingSQL.getInstance(symbol);
				} catch (Exception e) {
					System.err.println("no data for " + symbol);

					continue;
				}
				int datePosition = gsd.inDate.length - 1;
				while (gsd.inDate[datePosition].compareTo(mktDate) > 0)
					datePosition--;
				if (gsd.inDate[datePosition].compareTo(mktDate) != 0) {
					// System.out.println(mktDate + " not found for ");
					continue;
				}

				String guesses = rsGetGuesses.getString(2);
				String rowOfGuesses[] = guesses.split(";");

				for (int r = 1; r <= rowOfGuesses.length; r++) {

					int testDatePosition = datePosition + r;
					if (testDatePosition >= gsd.inDate.length)
						continue nextGuess;

					double got = gsd.inClose[testDatePosition] / gsd.inClose[datePosition];
					int daysOut = r;
					// System.out.println(gsd.inDate[testDatePosition] + ":" + daysOut + ":" +
					// gsd.inDate[datePosition]);
					Averager allDayAvg = allDaysAverage.get(daysOut);
					if (allDayAvg == null) {
						allDayAvg = new Averager();
						allDaysAverage.put(daysOut, allDayAvg);

					}
					Averager sheetDayAvg = sheetDaysAverage.get(daysOut);
					if (sheetDayAvg == null) {
						sheetDayAvg = new Averager();
						sheetDaysAverage.put(daysOut, sheetDayAvg);

					}
					if (rowOfGuesses[r - 1].contains("?"))
						continue;

					Double cellgot;
					try {
						cellgot = Double.parseDouble(rowOfGuesses[r - 1]);

					} catch (Exception e) {
						continue;

					}

					if (cellgot > CorrelationEstimator.buyIndicatorLimit) {
						buyPctChange.add(got);
						if (got > 1) {
							rightWrong.add(1);
							buyRightWrong.add(1);
							allDayAvg.add(1);
							sheetDayAvg.add(1);
						} else {
							rightWrong.add(0);
							buyRightWrong.add(0);
							allDayAvg.add(0);
							sheetDayAvg.add(0);
						}

					} else if (cellgot < CorrelationEstimator.sellIndicatorLimit) {
						sellPctChange.add(got);
						if (got < 1) {
							rightWrong.add(1);
							sellRightWrong.add(1);
							allDayAvg.add(1);
							sheetDayAvg.add(1);

						} else {
							rightWrong.add(0);
							sellRightWrong.add(0);
							allDayAvg.add(0);
							sheetDayAvg.add(0);

						}

					}

				}
			}
			// System.out.println();

			sb.append("<fo:table-row > " + "<fo:table-cell><fo:block>" + mktDate + "</fo:block></fo:table-cell>\n");

			sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(rightWrong, false, avgRightWrong)
					+ "</fo:block></fo:table-cell>\n");

			sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(buyRightWrong, false, avgBuyRightWrong)
					+ "</fo:block></fo:table-cell>\n");

			sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(sellRightWrong, false, avgSellRightWrong)
					+ "</fo:block></fo:table-cell>\n");

			sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(buyPctChange, true, avgBuyPctChange)
					+ "</fo:block></fo:table-cell>\n");

			sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(sellPctChange, true, avgSellPctChange)
					+ "</fo:block></fo:table-cell>\n");

			sb.append("</fo:table-row>");

		}

		sb.append("<fo:table-row > " + "<fo:table-cell><fo:block>Average</fo:block></fo:table-cell>\n");

		sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(avgRightWrong, false, null)
				+ "</fo:block></fo:table-cell>\n");

		sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(avgBuyRightWrong, false, null)
				+ "</fo:block></fo:table-cell>\n");

		sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(avgSellRightWrong, false, null)
				+ "</fo:block></fo:table-cell>\n");

		sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(avgBuyPctChange, true, null)
				+ "</fo:block></fo:table-cell>\n");

		sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(avgSellPctChange, true, null)
				+ "</fo:block></fo:table-cell>\n");

		sb.append("</fo:table-row>");

		allText = allText.replace("<!--   <<<<% correct for calls for date>>>> -->", sb.toString());
		allText = allText.replace("<<<<Current Report Date>>>>", rptDate);
		sb = new StringBuffer();
		for (String key : allSheetsDaysAverage.keySet()) {
			TreeMap<Integer, Averager> sheetDaysAverage = allSheetsDaysAverage.get(key);

			sb.append("<fo:table-row > " + "<fo:table-cell number-columns-spanned='2'><fo:block>" + key
					+ "</fo:block></fo:table-cell>\n");

			for (Integer daI : sheetDaysAverage.keySet()) {
				sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(sheetDaysAverage.get(daI), false, null)
						+ "</fo:block></fo:table-cell>\n");
			}
			sb.append("</fo:table-row>");
		}
		sb.append("<fo:table-row > "
				+ "<fo:table-cell number-columns-spanned='2'><fo:block>All:</fo:block></fo:table-cell>\n");
		for (Integer daI : allDaysAverage.keySet()) {
			sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(allDaysAverage.get(daI), false, null)
					+ "</fo:block></fo:table-cell>\n");
		}
		sb.append("</fo:table-row>");

		allText = allText.replace("<!--   <<<<data days out % call correct by date>>>> -->", sb.toString());
		allText = allText.replace("<<<<COPYRIGHT YEAR>>>>", currentMktDate.substring(0, 4));
		PrintWriter prePDF = new PrintWriter("c:/users/joe/correlationOutput/prepdf.xml");
		prePDF.println(allText);
		prePDF.flush();
		prePDF.close();
		String rptName = PDFPerformanceReport
				.makeReport(new ByteArrayInputStream(allText.getBytes(StandardCharsets.UTF_8)), rptDate);

//		try {
//
//			FTPClient ftpc = new FTPClient();
//			ftpc.connect("mcverryreport.com");
//
//			ftpc.login("reportly@mcverryreport.com", "*gg77_3*g168");
//
//			ftpc.setControlKeepAliveTimeout(500);
//
//			File f = new File(rptName);
//
//			PDFMergerUtility merger = new PDFMergerUtility();
//			merger.setDestinationFileName("c:/users/joe/correlationOutput/reportPerformanceOnly.pdf");
//			merger.addSource(f);
//			merger.addSource(f);
//
//			merger.mergeDocuments(MemoryUsageSetting.setupMainMemoryOnly());
//
//			ftpc.storeFile("subscribe/reportPerformance2.pdf",
//					new FileInputStream("c:/users/joe/correlationOutput/reportPerformanceOnly.pdf"));
//
////			boolean completed = ftpc.completePendingCommand();
////			if (completed) {
////				System.out.println("The second file is uploaded successfully.");
////			}
//
//			ftpc.disconnect();
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

		return rptName;

	}

	static DecimalFormat df = new DecimalFormat("#%");
	static DecimalFormat dfp = new DecimalFormat("#.00%");

	static String rightWrongPercentage(Averager rightWrong, boolean setDecimal, Averager myAverager) {
		if (rightWrong.getCount() == 0)
			return "n/c";
		if (myAverager != null)
			myAverager.add(rightWrong.get());
		if (setDecimal)
			return dfp.format(rightWrong.get() - 1);
		return df.format(rightWrong.get());

	}

}
