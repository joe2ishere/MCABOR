package correlation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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
		adx.makeARFFFromSQL(sym, dos);
	}

	public String getFilename(String sym, String dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_adx_correlation.arff";
	}

	public TreeMap<String, Integer> dateAttribute = new TreeMap<>();

	public void makeARFFFromSQL(String sym, String dos) throws Exception {

		boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5
		Core core = new Core();
		Connection conn = null;

		conn = getDatabaseConnection.makeConnection();

		PreparedStatement ps = conn
				.prepareStatement("select * from adx_correlation" + " where symbol=? and toCloseDays=?");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		String startDate = gsd.inDate[50];

		TreeMap<String, double[]> adxs = new TreeMap<>();
		TreeMap<String, Integer> adxfunctionDaysDiff = new TreeMap<>();
		TreeMap<String, Integer> doubleBacks = new TreeMap<>();

		TreeMap<String, String[]> adxDates = new TreeMap<>();
		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		String in;
		PrintWriter pw = new PrintWriter(getFilename(sym, dos));
		pw.println("% 1. Title: " + sym + "_adx_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL gsdADX = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(gsdADX.inDate[50]) < 0) {
				startDate = gsdADX.inDate[50];
				System.out.println(startDate + " " + functionSymbol);
			}

			int period = rs.getInt("period");

			double adx[] = new double[gsdADX.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.adx(0, gsdADX.inClose.length - 1, gsdADX.inHigh, gsdADX.inLow, gsdADX.inClose, period, outBegIdx,
					outNBElement, adx);

			Realign.realign(adx, outBegIdx.value);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");

			adxs.put(symKey, adx);
			doubleBacks.put(symKey, rs.getInt("doubleBack"));
			adxfunctionDaysDiff.put(symKey, rs.getInt("functionDaysDiff"));
			pw.println("@ATTRIBUTE " + symKey + "adx NUMERIC");
			// pw.println("@ATTRIBUTE " + symKey + "adxback NUMERIC");
			adxDates.put(symKey, gsdADX.inDate);
		}
		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int arraypos[] = new int[adxs.size()];
		int pos = 0;
		for (String key : adxs.keySet()) {
			arraypos[pos] = 0;
		}
		pos = 50;
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		int attributePos = -1;
		eemindexLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut - 1;) {
			String posDate = gsd.inDate[iday];
			pos = 0;
			for (String key : adxs.keySet()) {
				String sdate = adxDates.get(key)[arraypos[pos]];
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
			for (String key : adxs.keySet()) {
				int giDay = iday - adxfunctionDaysDiff.get(key);
				int siDay = arraypos[pos] - adxfunctionDaysDiff.get(key);

				for (int i = 0; i < (adxfunctionDaysDiff.get(key) + daysOut); i++) {
					String gtest = gsd.inDate[giDay + i];
					String stest = adxDates.get(key)[siDay + i];
					if (gtest.compareTo(stest) != 0) {
						iday++;
						continue eemindexLoop;
					}
				}
				pos++;
			}
			Date now = null;
			try {
				now = sdf.parse(posDate);
			} catch (Exception e) {
				System.out.println(e.getLocalizedMessage() + ";" + posDate);
				e.printStackTrace();
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

//			ADXMakeARFFfromCorrelationFile.printAttributeData(iday, daysOut, pw, adxs, adxfunctionDaysDiff, arraypos, gsd.inClose,
//					bavg, savg);
			printAttributeData(iday, daysOut, pw, adxs, adxfunctionDaysDiff, doubleBacks, arraypos, gsd.inClose, db,
					withAttributePosition);
			dateAttribute.put(gsd.inDate[iday], ++attributePos);
			iday++;
			for (pos = 0; pos < arraypos.length; pos++) {
				arraypos[pos]++;
			}

		}

		pw.close();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public String getAttributeText(double adx[], int adxfunctionDaysDiff, int adxstart, int doubleBack) {
		return (adx[adxstart - doubleBack] + adx[adxstart - (adxfunctionDaysDiff + doubleBack)]) / 2 + ",";

	}

	public void printAttributeData(int iday, int daysOut, PrintWriter pw, Object inParms,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition) {

		TreeMap<String, double[]> adxs = (TreeMap<String, double[]>) inParms;
		int pos = 0;
		for (String key : adxs.keySet()) {

			double adx[] = adxs.get(key);
			int adxfunctionDaysDiff = functionDaysDiffMap.get(key);
			int adxstart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getAttributeText(adx, adxfunctionDaysDiff, adxstart, doubleBack.intValue()));

			pos++;

		}
		if (withAttributePosition)
			pw.println(priceBands.getAttributePosition(iday, daysOut, closes));
		else
			pw.println(priceBands.getAttributeValue(iday, daysOut, closes));

	}

}
