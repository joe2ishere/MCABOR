package correlation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;

import bands.DeltaBands;
import util.Realign;
import util.getDatabaseConnection;

public class ADXMakeARFFfromSQL {

	public static void main(String[] args) throws Exception {

		LogManager.getLogManager().reset();

		Scanner sin = new Scanner(System.in);
		String sym;
		while (true) {
			System.out.print("\nSymbol: ");
			sym = sin.nextLine().trim();

			if (sym == null | sym.length() < 1) {
				sin.close();
				return;
			}
			break;
		}

		String dos;
		while (true) {
			System.out.print("\ndays out: ");
			dos = sin.nextLine().trim();

			if (dos == null | dos.length() < 1) {
				sin.close();
				return;
			}
			break;
		}
		sin.close();
		ADXMakeARFFfromSQL adx = new ADXMakeARFFfromSQL();
		Connection conn = null;
		adx.makeARFFFromSQL(sym, dos, conn);
	}

	public String getFilename(String sym, String dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_adx_correlation.arff";
	}

	public TreeMap<String, Integer> dateAttribute = new TreeMap<>();

	public void makeARFFFromSQL(String sym, String dos, Connection conn) throws Exception {

		boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
		ADXParms adxp = new ADXParms();
		Core core = new Core();

		conn = getDatabaseConnection.makeConnection();

		PreparedStatement ps = conn
				.prepareStatement("select * from adx_correlation" + " where symbol=? and toCloseDays=?");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		String startDate = gsd.inDate[50];

		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		PrintWriter pw = new PrintWriter(getFilename(sym, dos));
		pw.println("% 1. Title: " + sym + "_adx_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL gsdADX = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(gsdADX.inDate[50]) < 0) {
				startDate = gsdADX.inDate[50];
			}

			int period = rs.getInt("period");

			double adx[] = new double[gsdADX.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.adx(0, gsdADX.inClose.length - 1, gsdADX.inHigh, gsdADX.inLow, gsdADX.inClose, period, outBegIdx,
					outNBElement, adx);

			Realign.realign(adx, outBegIdx.value);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");

			adxp.addSymbol(symKey);
			adxp.setADXS(symKey, adx);
			adxp.setDoubleBacks(symKey, rs.getInt("doubleBack"));
			adxp.setDaysDiff(symKey, rs.getInt("functionDaysDiff"));
			adxp.setAttrDates(symKey, gsdADX.inDate);

			pw.println("@ATTRIBUTE " + symKey + "adx NUMERIC");
			// pw.println("@ATTRIBUTE " + symKey + "adxback NUMERIC");
		}
		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");
		int pos = 50;
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		for (int iday = pos; iday < gsd.inDate.length - daysOut - 1; iday++) {

			StringBuffer sb = printAttributeData(iday, gsd.inDate, daysOut, adxp, gsd.inClose, db,
					withAttributePosition, false);
			if (sb != null)
				pw.print(sb.toString());

		}

		pw.close();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(double adx[], int adxfunctionDaysDiff, int adxstart, int doubleBack) {
		return (adx[adxstart - doubleBack] + adx[adxstart - (adxfunctionDaysDiff + doubleBack)]) / 2 + ",";

	}

	public StringBuffer printAttributeData(int iday, String etfDates[], int daysOut, ADXParms adxp, double[] closes,
			DeltaBands priceBands, boolean withAttributePosition, boolean forLastUseQuestionMark) {

		StringBuffer returnBuffer = new StringBuffer(1000);

		for (String key : adxp.keySet()) {
			String dates[] = adxp.getAttrDates(key);
			int adxstart = adxp.getLastDateStart(key);
			for (; adxstart < dates.length; adxstart++) {
				if (dates[adxstart].compareTo(etfDates[iday]) == 0)
					break;
				if (dates[adxstart].compareTo(etfDates[iday]) > 0)
					return null;
			}
			int adxfunctionDaysDiff = adxp.getDaysDiff(key);
			Integer doubleBack = adxp.getDoubleBacks(key);
			// are we missing any days in between
			int giDay = iday - adxfunctionDaysDiff;
			int siDay = adxstart - adxfunctionDaysDiff;
			// are we missing any days in between
			for (int i = 0; i < (adxfunctionDaysDiff + daysOut); i++) {
				String gtest = etfDates[giDay + i];
				String stest = dates[siDay + i];
				if (gtest.compareTo(stest) != 0) {
					return null;
				}
			}
			adxp.setLastDateStart(key, adxstart);
			double adxs[] = adxp.getADXS(key);

			if (doubleBack == null)
				doubleBack = 0;
			returnBuffer.append(getAttributeText(adxs, adxfunctionDaysDiff, adxstart, doubleBack.intValue()));

		}
		if (forLastUseQuestionMark)
			returnBuffer.append("?");
		else if (withAttributePosition)
			returnBuffer.append(priceBands.getAttributePosition(iday, daysOut, closes));
		else
			returnBuffer.append(priceBands.getAttributeValue(iday, daysOut, closes));

		returnBuffer.append("\n");
		return returnBuffer;
	}

}
