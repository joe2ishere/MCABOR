package correlation;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

public class PricePlanSpreadSheetReport {

	public static void main(String[] args) throws Exception {

		Calendar cd = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yy");
		String today = sdf.format(cd.getTime());

		Workbook activeWB = Workbook.getWorkbook(new FileInputStream("/users/joe/documents/stockplan.xls"));

		Sheet activeSheet = activeWB.getSheet("Active");

		TreeMap<String, String[]> symbolAndDates = new TreeMap<>();
		TreeMap<String, Double> symbolMax = new TreeMap<>();
		Cell[] symbolColumn = activeSheet.getColumn(1);
		Cell[] priceBot = activeSheet.getColumn(4);
		Cell[] startSellColumn = activeSheet.getColumn(6);
		Cell[] endSellColumn = activeSheet.getColumn(7);
		Cell[] sellPriceColumn = activeSheet.getColumn(8);

		for (int row = 2; row < symbolColumn.length; row++) {

			String symbol = symbolColumn[row].getContents().trim();
			if (symbol == null)
				continue;
			if (symbol.length() == 0)
				continue;
			if (row < sellPriceColumn.length) {
				String sellPrice = sellPriceColumn[row].getContents();
				if (sellPrice != null) {
					sellPrice = sellPrice.trim();
					if (sellPrice.length() > 0) {
						System.out.println("sold " + symbol);
						continue;
					}
				}
			}

			String[] dates = symbolAndDates.get(symbol);
			if (dates == null) {
				dates = new String[2];
				dates[0] = startSellColumn[row].getContents();
				dates[1] = endSellColumn[row].getContents();
				symbolAndDates.put(symbol, dates);

				if (row < priceBot.length) {
					double d = Double.parseDouble(priceBot[row].getContents());
					symbolMax.put(symbol, d);
				} else
					symbolMax.put(symbol, 0.);
				continue;
			}
			if (dates[0].compareTo(startSellColumn[row].getContents()) > 0)
				dates[0] = startSellColumn[row].getContents();
			if (dates[1].compareTo(endSellColumn[row].getContents()) > 0)
				dates[1] = endSellColumn[row].getContents();
			if (row < priceBot.length)
				if (priceBot[row].getContents().trim().length() > 0) {
					double d = Double.parseDouble(priceBot[row].getContents());

					symbolMax.put(symbol, Math.max(symbolMax.get(symbol), d));
				}
		}

		System.out.println("Symbol;Start Dt;Mid Point;End Dt");
		for (String sym : symbolAndDates.keySet()) {
			Date dLow = sdf.parse(symbolAndDates.get(sym)[0]);
			Date dHigh = sdf.parse(symbolAndDates.get(sym)[1]);
			Date dMid = new Date((dHigh.getTime() + dLow.getTime()) / 2);
			String dMidFmt = sdf.format(dMid);
			System.out.print(sym + ";" + symbolAndDates.get(sym)[0] + ";" + dMidFmt + ";" + symbolAndDates.get(sym)[1]
					+ ";" + symbolMax.get(sym));
			if (today.compareTo(symbolAndDates.get(sym)[0]) >= 0 & today.compareTo(symbolAndDates.get(sym)[1]) <= 0) {

				if (today.compareTo(dMidFmt) >= 0)
					System.out.println(" <<<< ====  TIME TO SELL");
				else
					System.out.println(" <<<< ====  Time to sell");
			} else
				System.out.println();
		}

	}
}
