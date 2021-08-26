package correlation;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import util.getDatabaseConnection;

public class MoveSpreadSheetToSql {

	public static void main(String[] args) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();
		PreparedStatement ps = conn
				.prepareStatement("insert into correlation30days (mktDate, symbol, guesses) values(?,?,?)");
		Workbook wb = Workbook.getWorkbook(new FileInputStream("compareCorrelations.xls"));

		giveASheet: for (Sheet sheet : wb.getSheets()) {
			String sheetDate = sheet.getName();
//			if (sheetDate.compareTo("2020-03-10") < 0)
//				continue;
			System.out.print(sheetDate + ";");
			//TreeMap<Integer, Averager> sheetDaysAverage = new TreeMap<>();

			Cell[] cellColumn = sheet.getColumn(0);
			int row = -1;

			moveToNextRow: for (Cell cell : cellColumn) {
				row++;
				Cell[] rows = sheet.getRow(row);
				if (cell.getContents().startsWith("Daily")) {

					continue;
				}

				String symbol = cell.getContents();
				StringBuffer sb = new StringBuffer();
				for (int r = 1; r < 31; r++) {
					Cell cellRow = rows[r];
					sb.append(cellRow.getContents());
					if (r < 30)
						sb.append(";");
				}
				ps.setString(1, sheetDate);
				ps.setString(2, symbol);
				ps.setString(3, sb.toString());
				ps.execute();
				System.out.println(sheetDate + symbol + sb.toString());

			}
		}
	}
}
