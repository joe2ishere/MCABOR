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
import com.tictactec.ta.lib.MAType;

import StochasticMomentum.StochasticMomentum;
import bands.DeltaBands;
import util.Averager;
import util.getDatabaseConnection;

public class SMMakeARFFfromSQL {

	boolean withAttributePosition = false; // position is 0 through 9 otherwise it's n1 through p5

	public SMMakeARFFfromSQL(boolean b) {
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
		SMMakeARFFfromSQL sm = new SMMakeARFFfromSQL(false);
		sm.makeARFFFromSQL(sym, dos);
	}

	public String getFilename(String sym, String daysOut) {
		return "c:/users/joe/correlationARFF/" + sym + "_" + daysOut + "_smi_correlation.arff";
	}

	public TreeMap<String, Integer> dateAttribute = new TreeMap<>();

	public void makeARFFFromSQL(String sym, String dos) throws Exception {

		Connection conn = null;

		conn = getDatabaseConnection.makeConnection();

		PreparedStatement ps = conn
				.prepareStatement("select * from sm_correlation" + " where symbol=?  and  toCloseDays=?");

		int daysOut = Integer.parseInt(dos);

		Averager bavg = new Averager();
		Averager savg = new Averager();

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
		String startDate = gsd.inDate[50];
		DeltaBands db = new DeltaBands(gsd.inClose, daysOut, 5);

		TreeMap<String, Object> smis = new TreeMap<>();
		TreeMap<String, Integer> smfunctionDaysDiff = new TreeMap<>();
		TreeMap<String, Integer> doubleBacks = new TreeMap<>();
		TreeMap<String, String[]> smDates = new TreeMap<>();
		ps.setString(1, sym);
		ps.setInt(2, daysOut);
		ResultSet rs = ps.executeQuery();

		PrintWriter pw = new PrintWriter(getFilename(sym, dos));
		pw.println("% 1. Title: " + sym + "_smi_correlation");
		pw.println("@RELATION " + sym + "_" + dos);

		while (rs.next()) {

			String functionSymbol = rs.getString("functionSymbol");

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(functionSymbol);

			if (startDate.compareTo(pgsd.inDate[50]) < 0)
				startDate = pgsd.inDate[50];

			int hiLowPeriod = rs.getInt("hiLowPeriod");
			int maPeriod = rs.getInt("maPeriod");
			int smSmoothPeriod = rs.getInt("smSmoothPeriod");
			int smSignalPeriod = rs.getInt("smSignalPeriod");
			StochasticMomentum sm = new StochasticMomentum(pgsd.inHigh, pgsd.inLow, pgsd.inClose, hiLowPeriod,
					MAType.Ema, maPeriod, smSmoothPeriod, smSignalPeriod);
			String symKey = functionSymbol + "_" + rs.getInt("significantPlace");
			smis.put(symKey, sm);
			doubleBacks.put(symKey, rs.getInt("doubleBack"));
			smfunctionDaysDiff.put(symKey, rs.getInt("functionDaysDiff"));
			pw.println("@ATTRIBUTE " + symKey + "smi NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "signal NUMERIC");

			smDates.put(symKey, pgsd.inDate);

		}

		if (withAttributePosition)
			pw.println("@ATTRIBUTE class NUMERIC");
		else
			pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int arraypos[] = new int[smis.size()];
		int pos = 50;
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;
		int attributePos = -1;
		eemindexLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut;) {
			String posDate = gsd.inDate[iday];
			pos = 0;
			for (String key : smis.keySet()) {
				String sdate = smDates.get(key)[arraypos[pos]];
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
			for (String key : smis.keySet()) {
				int giDay = iday - smfunctionDaysDiff.get(key);
				int siDay = arraypos[pos] - smfunctionDaysDiff.get(key);

				for (int i = 0; i < (smfunctionDaysDiff.get(key) + daysOut); i++) {
					String gtest = gsd.inDate[giDay + i];
					String stest = smDates.get(key)[siDay + i];
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

//			SMMakeARFFfromCorrelationFile.printAttributeData(iday, daysOut, pw, smis, smfunctionDaysDiff, arraypos, gsd.inClose,
//					bavg, savg);
			printAttributeData(iday, daysOut, pw, smis, smfunctionDaysDiff, doubleBacks, arraypos, gsd.inClose, db,
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

	public String getAttributeText(StochasticMomentum sm, int smfunctionDaysDiff, int smstart, int doubleBack) {
		return (sm.SMI[smstart] + "," + sm.Signal[smstart] + ",");

	}

	public void printAttributeData(int iday, int daysOut, PrintWriter pw, TreeMap<String, Object> myParms,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] closes, DeltaBands priceBands, boolean withAttributePosition) {

		int pos = 0;
		for (String key : myParms.keySet()) {
			StochasticMomentum sm = (StochasticMomentum) myParms.get(key);
			int smfunctionDaysDiff = functionDaysDiffMap.get(key);
			int smstart = arraypos[pos];
			// System.out.print(smDates.get(key)[smstart] + ";");
			pw.print(getAttributeText(sm, smfunctionDaysDiff, smstart, 0));
			pos++;
		}
		if (withAttributePosition)
			pw.println(priceBands.getAttributePosition(iday, daysOut, closes));
		else
			pw.println(priceBands.getAttributeValue(iday, daysOut, closes));

		pw.flush();

	}

}
