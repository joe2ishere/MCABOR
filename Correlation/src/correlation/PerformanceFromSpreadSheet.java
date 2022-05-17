package correlation;

import java.io.File;
import java.io.FileInputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import correlation.Estimators.CorrelationEstimator;
import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import util.Averager;

public class PerformanceFromSpreadSheet {

	public static void main(String[] args) throws Exception {

		System.out.println("Date;Total avg;Buy avg;Sell avg;Actual Buy % chg;Actual Sell % chg");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		TreeMap<Integer, Averager> allDaysAverage = new TreeMap<>();
		TreeMap<String, TreeMap<Integer, Averager>> allSheetsDaysAverage = new TreeMap<>();
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
//			if (sheetDate.compareTo("2020-03-10") < 0)
//				continue;
				System.out.print(sheetDate + ";");
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
				;
				System.out.println(rightWrongPercentage(rightWrong) + ";" + rightWrongPercentage(buyRightWrong) + ";"
						+ rightWrongPercentage(sellRightWrong) + ";" + rightWrongPercentage(buyPctChange) + ";"
						+ rightWrongPercentage(sellPctChange));
			}
		}
		System.out.println("All days out average % call correct");
		for (Integer daI : allDaysAverage.keySet()) {
			System.out.print(daI + ";");

		}
		System.out.println();

		for (Integer daI : allDaysAverage.keySet()) {
			System.out.print(rightWrongPercentage(allDaysAverage.get(daI)) + ";");
		}

		System.out.println("\n All dates average % call correct");

		for (String key : allSheetsDaysAverage.keySet()) {
			TreeMap<Integer, Averager> sheetDaysAverage = allSheetsDaysAverage.get(key);
			System.out.print(key + ";");

			for (Integer daI : sheetDaysAverage.keySet()) {
				System.out.print(rightWrongPercentage(sheetDaysAverage.get(daI)) + ";");
			}
			System.out.println();
		}

	}

	static DecimalFormat df = new DecimalFormat("#%");

	static String rightWrongPercentage(Averager rightWrong) {
		if (rightWrong.getCount() == 0)
			return "n/c";
		return df.format(rightWrong.get());

	}

}
