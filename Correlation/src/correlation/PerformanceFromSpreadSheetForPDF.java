package correlation;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import util.Averager;

public class PerformanceFromSpreadSheetForPDF {

	public static void main(String[] args) throws Exception {
		makeReport();
	}

	public static String makeReport() throws Exception {

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
		File spreadSheetDir = new File("c:/users/joe/correlationSpreadsheets");
		File files[] = spreadSheetDir.listFiles();

		for (File spreadSheet : files) {

			if (spreadSheet.getName().contains("lock") == true)
				continue;
			if (spreadSheet.getName().contains("-202") == false)
				continue;

			Workbook wb = Workbook.getWorkbook(new FileInputStream(spreadSheet));

			/* giveASheet: */ for (Sheet sheet : wb.getSheets()) {
				String sheetDate = sheet.getName();
				if (rptDate == null)
					rptDate = sheetDate;

				TreeMap<Integer, Averager> sheetDaysAverage = new TreeMap<>();
				allSheetsDaysAverage.put(sheetDate, sheetDaysAverage);

				Date sheetDt = sdf.parse(sheetDate);
				Calendar sheetClndr = Calendar.getInstance();
				sheetClndr.setTime(sheetDt);
				Cell[] cellColumn = sheet.getColumn(0);
				int row = -1;
				Averager rightWrong = new Averager();
				Averager buyRightWrong = new Averager();
				Averager sellRightWrong = new Averager();
				Averager buyPctChange = new Averager();
				Averager sellPctChange = new Averager();
				int maxCol = 15;
				int incrementForMissing1 = 1;

				moveToNextRow: for (Cell cell : cellColumn) {
					row++;
					Cell[] rows = sheet.getRow(row);
					if (cell.getContents().startsWith("Daily")) {
						if (rows[15].getContents().startsWith("D.")) {
							maxCol = 31;
							incrementForMissing1 = 0;
						}

						continue;
					}

					String symbol = cell.getContents();
//				System.out.print(symbol);
					GetETFDataUsingSQL gsd;
					try {
						gsd = GetETFDataUsingSQL.getInstance(symbol);
					} catch (Exception e) {
						continue moveToNextRow;
					}

					int datePosition = gsd.inDate.length - 1;
					while (gsd.inDate[datePosition].compareTo(sheetDate) > 0)
						datePosition--;
					if (gsd.inDate[datePosition].compareTo(sheetDate) != 0) {
						// System.out.println(sheetDate + " not found for ");
						continue;
					}

					for (int r = 1; r < maxCol; r++) {

						int testDatePosition = datePosition + r + incrementForMissing1;
						if (testDatePosition >= gsd.inDate.length)
							continue moveToNextRow;
						Cell cellRow = rows[r];
						double got = gsd.inClose[testDatePosition] / gsd.inClose[datePosition];
						int daysOut = testDatePosition - datePosition;
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
						Double cellgot = Double.parseDouble(cellRow.getContents());
						if (cellgot == 0)
							continue;
						if (got > 1)
							buyPctChange.add(got);
						else
							sellPctChange.add(got);

						if (cellgot > CorrelationEstimator.buyIndicatorLimit) {

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
					// System.out.println();

				}
				sb.append(
						"<fo:table-row > " + "<fo:table-cell><fo:block>" + sheetDate + "</fo:block></fo:table-cell>\n");

				sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(rightWrong, false)
						+ "</fo:block></fo:table-cell>\n");
				sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(buyRightWrong, false)
						+ "</fo:block></fo:table-cell>\n");
				sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(sellRightWrong, false)
						+ "</fo:block></fo:table-cell>\n");
				sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(buyPctChange, true)
						+ "</fo:block></fo:table-cell>\n");
				sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(sellPctChange, true)
						+ "</fo:block></fo:table-cell>\n");
				sb.append("</fo:table-row>");
			}
		}
		allText = allText.replace("<!--   <<<<% correct for calls for date>>>> -->", sb.toString());
		allText = allText.replace("<<<<Current Report Date>>>>", rptDate);
		sb = new StringBuffer();
		for (String key : allSheetsDaysAverage.keySet()) {
			TreeMap<Integer, Averager> sheetDaysAverage = allSheetsDaysAverage.get(key);

			sb.append("<fo:table-row > " + "<fo:table-cell number-columns-spanned='2'><fo:block>" + key
					+ "</fo:block></fo:table-cell>\n");

			for (Integer daI : sheetDaysAverage.keySet()) {
				sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(sheetDaysAverage.get(daI), false)
						+ "</fo:block></fo:table-cell>\n");
			}
			sb.append("</fo:table-row>");
		}
		sb.append("<fo:table-row > "
				+ "<fo:table-cell number-columns-spanned='2'><fo:block>All:</fo:block></fo:table-cell>\n");
		for (Integer daI : allDaysAverage.keySet()) {
			sb.append("<fo:table-cell><fo:block>" + rightWrongPercentage(allDaysAverage.get(daI), false)
					+ "</fo:block></fo:table-cell>\n");
		}
		sb.append("</fo:table-row>");

		allText = allText.replace("<!--   <<<<data days out % call correct by date>>>> -->", sb.toString());
		allText = allText.replace("<<<<COPYRIGHT YEAR>>>>", "2021");

		PrintWriter pwtemp = new PrintWriter("xmlFilesForPDFReports/Report-Performance-FO.beforeprocessing.xml");
		pwtemp.print(allText);
		pwtemp.flush();
		pwtemp.close();
		return PDFPerformanceReport.makeReport(new ByteArrayInputStream(allText.getBytes(StandardCharsets.UTF_8)),
				rptDate);

	}

	static DecimalFormat df = new DecimalFormat("#%");
	static DecimalFormat dfp = new DecimalFormat("#.00%");

	static String rightWrongPercentage(Averager rightWrong, boolean setDecimal) {
		if (rightWrong.getCount() == 0)
			return "n/c";
		if (setDecimal)
			return dfp.format(rightWrong.get() - 1);
		return df.format(rightWrong.get());

	}

}
