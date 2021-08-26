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

public class DMIMakeARFFfromSQL {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5

	public DMIMakeARFFfromSQL(boolean b) {
		withAttributePosition = b;
	}

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
		DMIMakeARFFfromSQL dmi = new DMIMakeARFFfromSQL(false);
		dmi.makeARFFFromSQL(sym, dos);
	}

	public String getFilename(String sym, String dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_dmi_correlation.arff";
	}

	public TreeMap<String, Integer> dateAttribute = new TreeMap<>();

	public void makeARFFFromSQL(String sym, String dos) throws Exception {

		Core core = new Core();
		Connection conn = null;

		conn = getDatabaseConnection.makeConnection();

		PreparedStatement ps = conn
				.prepareStatement("select * from dmi_correlation" + " where symbol=? and toCloseDays=?  ");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		String startDate = gsd.inDate[50];

		TreeMap<String, double[]> dmis = new TreeMap<>();
		TreeMap<String, Integer> dmifunctionDaysDiff = new TreeMap<>();
		TreeMap<String, Integer> doubleBacks = new TreeMap<>();

		TreeMap<String, String[]> dmiDates = new TreeMap<>();
		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		String in;
		PrintWriter pw = new PrintWriter(getFilename(sym, dos));
		pw.println("% 1. Title: " + sym + "_dmi_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			double absCorr = Math.abs(rs.getDouble("correlation"));

			GetETFDataUsingSQL gsdDMI = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(gsdDMI.inDate[50]) < 0)
				startDate = gsdDMI.inDate[50];

			int dmiPeriod = rs.getInt("dmiPeriod");

			double dmi[] = new double[gsdDMI.inClose.length];
			MInteger outBegIdx = new MInteger();
			MInteger outNBElement = new MInteger();
			core.dx(0, gsdDMI.inClose.length - 1, gsdDMI.inHigh, gsdDMI.inLow, gsdDMI.inClose, dmiPeriod, outBegIdx,
					outNBElement, dmi);

			Realign.realign(dmi, outBegIdx.value);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");
			dmis.put(symKey, dmi);
			int doubleBack = rs.getInt("doubleBack");
			doubleBacks.put(symKey, doubleBack);
			dmifunctionDaysDiff.put(symKey, rs.getInt("functionDaysDiff"));
			pw.println("@ATTRIBUTE " + symKey + "dmi NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");
			dmiDates.put(symKey, gsdDMI.inDate);
		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int arraypos[] = new int[dmis.size()];
		int pos = 50;
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		int attributePos = -1;
		eemindexLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut - 1;) {
			String posDate = gsd.inDate[iday];
			pos = 0;
			for (String key : dmis.keySet()) {
				String sdate = dmiDates.get(key)[arraypos[pos]];
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
			for (String key : dmis.keySet()) {
				int giDay = iday - dmifunctionDaysDiff.get(key);
				int siDay = arraypos[pos] - dmifunctionDaysDiff.get(key);

				for (int i = 0; i < (dmifunctionDaysDiff.get(key) + daysOut); i++) {
					String gtest = gsd.inDate[giDay + i];
					String stest = dmiDates.get(key)[siDay + i];
					if (gtest.compareTo(stest) != 0) {
						iday++;
						continue eemindexLoop;
					}
				}
				pos++;
			}
			Date now = sdf.parse(posDate);

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

//			DMIMakeARFFfromCorrelationFile.printAttributeData(iday, daysOut, pw, dmis, dmifunctionDaysDiff, arraypos, gsd.inClose,
//					bavg, savg);
			printAttributeData(iday, daysOut, pw, dmis, dmifunctionDaysDiff, doubleBacks, arraypos, gsd.inClose, db,
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

	public String getAttributeText(double dmi[], int dmifunctionDaysDiff, int dmistart, int doubleBack) {
		return dmi[dmistart - doubleBack] + "," + dmi[dmistart - (dmifunctionDaysDiff + doubleBack)] + ",";

	}

	public void printAttributeData(int iday, int daysOut, PrintWriter pw, Object inParms,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition) {
		TreeMap<String, double[]> dmis = (TreeMap<String, double[]>) inParms;
		int pos = 0;
		for (String key : dmis.keySet()) {

			double dmi[] = dmis.get(key);
			int dmifunctionDaysDiff = functionDaysDiffMap.get(key);
			int dmistart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getAttributeText(dmi, dmifunctionDaysDiff, dmistart, doubleBack.intValue()));

			pos++;

		}
		if (withAttributePosition)
			pw.println(priceBands.getAttributePosition(iday, daysOut, closes));
		else
			pw.println(priceBands.getAttributeValue(iday, daysOut, closes));
	}

}
