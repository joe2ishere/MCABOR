package correlation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TreeMap;
import java.util.logging.LogManager;

import util.getDatabaseConnection;

public class BuildSQLTable {
	static boolean delete = false;

//	static String argnames[] = { "efu" };

	public static void main(String args[]) throws Exception {

//		adxmain(args);
//		atrmain(args);
////		bbmain(args);
////		ccimain(args);
//		dmimain(args);
////		keltnermain(args);
//		macdmain(args);
////		natrmain(args);
////
		smimain(args);
//		tsfmain(args);

	}

	public static void bbmain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into bb_correlation"
				+ "(symbol,   toCloseDays, significantPlace, functionSymbol, period, stddev, "
				+ "  functionDaysDiff, doubleBack, correlation)" + " values (?,?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader(DoubleBackBollBandsCorrelation.tabFile);
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("not set"))
				continue;
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement(
						"delete from bb_correlation" + " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}

			// 0:0.1659669430621573:aapl;1;sjnk;5;5;9;9;9;4;0.1659669430621573
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			// aapl;1;sjnk;5;5;9;9;9;4;0.1659669430621573
			// 1 2 3 4 5 6 7 8 9
			stInsert.setString(1, DoubleBackBollBandsCorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, DoubleBackBollBandsCorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackBollBandsCorrelation.getBollBandsSymbol(keyIn));
			stInsert.setInt(5, DoubleBackBollBandsCorrelation.getPeriod(keyIn));
			stInsert.setDouble(6, DoubleBackBollBandsCorrelation.getStdDev(keyIn));
			stInsert.setInt(7, DoubleBackBollBandsCorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(8, DoubleBackBollBandsCorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(9, DoubleBackBollBandsCorrelation.getCorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();

	}

	public static void dmimain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into dmi_correlation"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, dmiPeriod,  functionDaysDiff, doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader(DoubleBackDMICorrelation.tabFile);
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement(
						"delete from dmi_correlation" + " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackDMICorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, DoubleBackDMICorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackDMICorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, DoubleBackDMICorrelation.getDMIPeriod(keyIn));
			stInsert.setInt(6, DoubleBackDMICorrelation.getDMIDaysBack(keyIn));
			stInsert.setInt(7, DoubleBackDMICorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(8, DoubleBackDMICorrelation.getCorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();
	}

	public static void keltnermain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into keltner_correlation"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, maperiod,  atrperiod, emaMultiplier, "
				+ "functionDaysDiff, doubleBack, correlation)" + " values (?,?,?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader(DoubleBackKeltnerCorrelation.tabFile);
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("not set"))
				continue;
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement("delete from keltner_correlation"
						+ " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackKeltnerCorrelation.getCloseSym(keyIn));
			stInsert.setInt(2, DoubleBackKeltnerCorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackKeltnerCorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, DoubleBackKeltnerCorrelation.getMAPeriod(keyIn));
			stInsert.setInt(6, DoubleBackKeltnerCorrelation.getATRPeriod(keyIn));
			stInsert.setDouble(7, DoubleBackKeltnerCorrelation.getEMAMultiplier(keyIn));
			stInsert.setInt(8, DoubleBackKeltnerCorrelation.getKeltnerFunctionDaysDiff(keyIn));
			stInsert.setInt(9, DoubleBackKeltnerCorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(10, DoubleBackKeltnerCorrelation.getCorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();
	}

	public static void smimain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into sm_correlation"
				+ "(symbol,   toCloseDays, significantPlace, functionSymbol, hiLowPeriod, maPeriod, "
				+ "smSmoothPeriod, smSignalPeriod, functionDaysDiff, doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader(DoubleBackSMICorrelation.tabFile);
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement(
						"delete from sm_correlation" + " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}

			// 0:0.1659669430621573:aapl;1;sjnk;5;5;9;9;9;4;0.1659669430621573
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			// aapl;1;sjnk;5;5;9;9;9;4;0.1659669430621573
			// 1 2 3 4 5 6 7 8 9
			stInsert.setString(1, DoubleBackSMICorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, DoubleBackSMICorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackSMICorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, DoubleBackSMICorrelation.gethiLowPeriod(keyIn));
			stInsert.setInt(6, DoubleBackSMICorrelation.getmaPeriod(keyIn));
			stInsert.setInt(7, DoubleBackSMICorrelation.getsmSmoothPeriod(keyIn));
			stInsert.setInt(8, DoubleBackSMICorrelation.getsmSignalPeriod(keyIn));
			stInsert.setInt(9, DoubleBackSMICorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(10, DoubleBackSMICorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(11, DoubleBackSMICorrelation.getcorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();

	}

	public static void macdmain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into macd_correlation"
				+ "(symbol,   toCloseDays, significantPlace, functionSymbol, fastPeriod, slowPeriod, signalPeriod, functionDaysDiff,  doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader(DoubleBackMACDCorrelation.tabFile);
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement("delete from macd_correlation" + " where symbol = '"
						+ ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackMACDCorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, DoubleBackMACDCorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackMACDCorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, DoubleBackMACDCorrelation.getFast(keyIn));
			stInsert.setInt(6, DoubleBackMACDCorrelation.getSlow(keyIn));
			stInsert.setInt(7, DoubleBackMACDCorrelation.getSignal(keyIn));
			stInsert.setInt(8, DoubleBackMACDCorrelation.getMacdfunctionDaysDiff(keyIn));
			stInsert.setInt(9, DoubleBackMACDCorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(10, DoubleBackMACDCorrelation.getCorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();
	}

//	public static void ppomain(String[] args) throws Exception {
//		Connection conn = getDatabaseConnection.makeConnection();
//
//		LogManager.getLogManager().reset();
//
//		final PreparedStatement stInsert = conn.prepareStatement("insert into ppo_correlation"
//				+ "(symbol,  toCloseDays, significantPlace, ppoSymbol, fastPeriod, slowPeriod,   functionDaysDiff,doubleBack, correlation)"
//				+ " values (?,?,?,?,?,?,?,?,?)");
//
//		FileReader fr = new FileReader(DoubleBackPPOCorrelation.tabFile);
//		BufferedReader br = new BufferedReader(fr);
//		String in = "";
//
//		conn.setAutoCommit(false);
//		while ((in = br.readLine()) != null) {
//			if (in.contains("_")) {
//				String ins[] = in.split("_");
//				PreparedStatement stDelete = conn.prepareStatement(
//						"delete from ppo_correlation" + " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
//				stDelete.execute();
//				continue;
//			}
//			String inColon[] = in.split(":");
//			String keyIn = inColon[2];
//			stInsert.setString(1, PPOCorrelation.closeSym(keyIn));
//
//			stInsert.setInt(2, PPOCorrelation.pricefunctionDaysDiff(keyIn));
//			stInsert.setInt(3, Integer.parseInt(inColon[0]));
//			stInsert.setString(4, PPOCorrelation.ppoSym(keyIn));
//			stInsert.setInt(5, PPOCorrelation.fastPeriod(keyIn));
//			stInsert.setInt(6, PPOCorrelation.slowPeriod(keyIn));
//			stInsert.setInt(7, PPOCorrelation.ppoBack(keyIn));
//			stInsert.setInt(8, PPOCorrelation.getDoubleBack(keyIn));
//			stInsert.setDouble(9, PPOCorrelation.corr(keyIn));
//			stInsert.addBatch();
//		}
//		stInsert.executeBatch();
//		stInsert.close();
//		conn.commit();
//		conn.close();
//	}

	public static void adxmain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into adx_correlation"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, period, functionDaysDiff,doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader(DoubleBackADXCorrelation.tabFile);
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("not set"))
				continue;
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement(
						"delete from ADX_correlation" + " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackADXCorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, DoubleBackADXCorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackADXCorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, DoubleBackADXCorrelation.getPeriod(keyIn));
			stInsert.setInt(6, DoubleBackADXCorrelation.getATRfunctionDaysDiff(keyIn));
			stInsert.setInt(7, DoubleBackADXCorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(8, DoubleBackADXCorrelation.getCorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();
	}

	public static void atrmain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into atr_correlation"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, period, functionDaysDiff,doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader(DoubleBackATRCorrelation.tabFile);
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement(
						"delete from ATR_correlation" + " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackATRCorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, DoubleBackATRCorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackATRCorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, DoubleBackATRCorrelation.getPeriod(keyIn));
			stInsert.setInt(6, DoubleBackATRCorrelation.getATRfunctionDaysDiff(keyIn));
			stInsert.setInt(7, DoubleBackATRCorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(8, DoubleBackATRCorrelation.getCorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();
	}

	public static void ccimain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into cci_correlation"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, period, functionDaysDiff,doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader(DoubleBackCCICorrelation.tabFile);
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("not set"))
				continue;
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement(
						"delete from cci_correlation" + " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackCCICorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, DoubleBackCCICorrelation.getfunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackCCICorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, DoubleBackCCICorrelation.getPeriod(keyIn));
			stInsert.setInt(6, DoubleBackCCICorrelation.getCCIDaysBack(keyIn));
			stInsert.setInt(7, DoubleBackCCICorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(8, DoubleBackCCICorrelation.getCorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();
	}

	public static void natrmain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into natr_correlation"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, natrPeriod,  functionDaysDiff, doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader(DoubleBackNATRCorrelation.tabFile);
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement("delete from natr_correlation" + " where symbol = '"
						+ ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			if (in.contains("not set"))
				continue;
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackNATRCorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, DoubleBackNATRCorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackNATRCorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, DoubleBackNATRCorrelation.getNATRPeriod(keyIn));
			stInsert.setInt(6, DoubleBackNATRCorrelation.getNATRDaysBack(keyIn));
			stInsert.setInt(7, DoubleBackNATRCorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(8, DoubleBackNATRCorrelation.getCorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();
	}

	public static void tsfmain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into tsf_correlation"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, tsfPeriod,  functionDaysDiff, doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader(DoubleBackTSFCorrelation.tabFile);
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement(
						"delete from tsf_correlation" + " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackTSFCorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, DoubleBackTSFCorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackTSFCorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, DoubleBackTSFCorrelation.getTSFPeriod(keyIn));
			stInsert.setInt(6, DoubleBackTSFCorrelation.getTSFDaysBack(keyIn));
			stInsert.setInt(7, DoubleBackTSFCorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(8, DoubleBackTSFCorrelation.getCorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();
	}

	public static void cleanupmain(String args[]) {
		Connection conn;
		try {
			conn = getDatabaseConnection.makeConnection();

			String sym = args[0];
			String mainSymbolDelete = "delete from %s where symbol = '%s'";
			String functionSymbolDelete = "delete from %s where %s = '%s'";

			TreeMap<String, String> tablesAndFields = new TreeMap<>();

			tablesAndFields.put("macd_correlation", "functionSymbol");
			tablesAndFields.put("natr_correlation", "functionSymbol");

			tablesAndFields.put("ppo_correlation", "ppoSymbol");
			tablesAndFields.put("sm_correlation", "functionSymbol");
			tablesAndFields.put("tsf_correlation", "functionSymbol");

			for (String key : tablesAndFields.keySet()) {

				System.out.println(String.format(mainSymbolDelete, key, sym));
				PreparedStatement ps = conn.prepareStatement(String.format(mainSymbolDelete, key, sym));

				ps.execute();

				// conn.createStatement().execute(String.format(mainSymbolDelete, key, sym));
				ps = conn.prepareStatement(String.format(functionSymbolDelete, key, tablesAndFields.get(key), sym));
				ps.execute();

				// conn.createStatement().execute(String.format(functionSymbolDelete, key,
				// tablesAndFields.get(key), sym));

			}

			// not necessary all the time
			PreparedStatement ps = conn.prepareStatement("delete from etfprices where symbol = '" + sym + "'");
			ps.execute();
			// conn.createStatement().execute("delete from etfprices where symbol = '" +
			// sym + "'");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
