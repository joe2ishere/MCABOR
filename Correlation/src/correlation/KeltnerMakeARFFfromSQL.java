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
import com.tictactec.ta.lib.MAType;

import bands.DeltaBands;
import keltner.KeltnerChannel;
import util.getDatabaseConnection;

public class KeltnerMakeARFFfromSQL {

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
		KeltnerMakeARFFfromSQL keltner = new KeltnerMakeARFFfromSQL();
		keltner.makeARFFFromSQL(sym, dos);
	}

	public static String getFilename(String sym, String dos) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + dos + "_keltner_correlation.arff";
	}

	public TreeMap<String, Integer> dateAttribute = new TreeMap<>();

	public void makeARFFFromSQL(String sym, String dos) throws Exception {

		Core core = new Core();
		Connection conn = null;

		conn = getDatabaseConnection.makeConnection();

		PreparedStatement ps = conn
				.prepareStatement("select * from keltner_correlation" + " where symbol=? and toCloseDays=?  ");

		int daysOut = Integer.parseInt(dos);

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);
		String startDate = gsd.inDate[50];

		TreeMap<String, KeltnerChannel> keltners = new TreeMap<>();
		TreeMap<String, Integer> keltnerfunctionDaysDiff = new TreeMap<>();
		TreeMap<String, Integer> doubleBacks = new TreeMap<>();

		TreeMap<String, String[]> keltnerDates = new TreeMap<>();
		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		String in;
		PrintWriter pw = new PrintWriter(getFilename(sym, dos));
		pw.println("% 1. Title: " + sym + "_keltner_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL gsdKeltner = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(gsdKeltner.inDate[50]) < 0)
				startDate = gsdKeltner.inDate[50];

			int maPeriod = rs.getInt("maperiod");
			int atrPeriod = rs.getInt("atrperiod");
			int emaMultiplier = rs.getInt("emaMultiplier");

			KeltnerChannel kc = new KeltnerChannel(gsdKeltner.inHigh, gsdKeltner.inLow, gsdKeltner.inClose, MAType.Ema,
					maPeriod, atrPeriod, emaMultiplier);

			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");
			keltners.put(symKey, kc);
			int doubleBack = rs.getInt("doubleBack");
			doubleBacks.put(symKey, doubleBack);
			keltnerfunctionDaysDiff.put(symKey, rs.getInt("functionDaysDiff"));
			pw.println("@ATTRIBUTE " + symKey + "keltnerMiddle NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "keltnerUpperWithOffset NUMERIC");
			keltnerDates.put(symKey, gsdKeltner.inDate);
		}

//		pw.println(db.getAttributeDefinition());
		pw.println("@ATTRIBUTE class NUMERIC");

		pw.println("@DATA");

		int arraypos[] = new int[keltners.size()];
		int pos = 0;
		for (String key : keltners.keySet()) {
			arraypos[pos] = 0;
		}
		pos = 50;
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;

		int attributePos = -1;
		eemindexLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut - 1;) {
			String posDate = gsd.inDate[iday];
			pos = 0;
			for (String key : keltners.keySet()) {
				String sdate = keltnerDates.get(key)[arraypos[pos]];
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
			for (String key : keltners.keySet()) {
				int giDay = iday - keltnerfunctionDaysDiff.get(key);
				int siDay = arraypos[pos] - keltnerfunctionDaysDiff.get(key);

				for (int i = 0; i < (keltnerfunctionDaysDiff.get(key) + daysOut); i++) {
					String gtest = gsd.inDate[giDay + i];
					String stest = keltnerDates.get(key)[siDay + i];
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

//			KeltnerMakeARFFfromCorrelationFile.printAttributeData(iday, daysOut, pw, keltners, keltnerfunctionDaysDiff, arraypos, gsd.inClose,
//					bavg, savg);
			printAttributeData(iday, daysOut, pw, keltners, keltnerfunctionDaysDiff, doubleBacks, arraypos, gsd.inClose,
					db, true);
			dateAttribute.put(gsd.inDate[iday], ++attributePos);
			iday++;
			for (pos = 0; pos < arraypos.length; pos++) {
				arraypos[pos]++;
			}

		}

		pw.close();
	}

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	static public String getAttributeText(KeltnerChannel keltner, int keltnerfunctionDaysDiff, int keltnerstart,
			int doubleBack) {
		return keltner.kelterChannelUpper[keltnerstart] / keltner.kelterChannelMiddle[keltnerstart] + ","
				+ keltner.kelterChannelUpper[keltnerstart - (keltnerfunctionDaysDiff + doubleBack)]
						/ keltner.kelterChannelMiddle[keltnerstart]
				+ ",";

	}

	static public void printAttributeData(int iday, int daysOut, PrintWriter pw, Object inParms,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition) {
		TreeMap<String, KeltnerChannel> keltners = (TreeMap<String, KeltnerChannel>) inParms;
		int pos = 0;
		for (String key : keltners.keySet()) {

			KeltnerChannel keltner = keltners.get(key);
			int keltnerfunctionDaysDiff = functionDaysDiffMap.get(key);
			int keltnerstart = arraypos[pos];
			Integer doubleBack = doubleBacks.get(key);
			if (doubleBack == null)
				doubleBack = 0;
			pw.print(getAttributeText(keltner, keltnerfunctionDaysDiff, keltnerstart, doubleBack.intValue()));

			pos++;

		}
		if (withAttributePosition)
			pw.println(priceBands.getAttributePosition(iday, daysOut, closes));
		else
			pw.println(priceBands.getAttributeValue(iday, daysOut, closes));
	}

}
