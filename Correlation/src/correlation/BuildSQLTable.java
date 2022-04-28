package correlation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.LogManager;
import java.util.stream.Collectors;

import util.getDatabaseConnection;

public class BuildSQLTable {
	static boolean delete = false;

//	static String argnames[] = { "efu" };

	public static void main(String args[]) throws Exception {

		apomain(args);
//		atrmain(args);
//		dmimain(args);
//		malinemain(args);
//		macdmain(args);
//		psarmain(args);
//		smimain(args);
//		tsfmain(args);
//		mamain(args);

//		dmimain_130(args);
////		malinemain_????(args);
//		macdmain_130(args);
//
//		smimain_130(args);
//		tsfmain_130(args);

	}

	public static List<String> getFileData(File file) throws IOException {
		return Files.lines(Paths.get(file.getPath())).filter(x -> !x.contains("set")).collect(Collectors.toList());
	}

	public static void atrmain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into atr_correlation"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, atrPeriod,  functionDaysDiff, doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?)");

		List<String> inFileData = getFileData(DoubleBackATRCorrelation.tabFile);

		conn.setAutoCommit(false);
		for (String in : inFileData) {
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement(
						"delete from atr_correlation" + " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
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
			stInsert.setInt(6, DoubleBackATRCorrelation.getFunctionDaysDiff(keyIn));
			stInsert.setInt(7, DoubleBackATRCorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(8, DoubleBackATRCorrelation.getCorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();
	}

	public static void apomain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into apo_correlation"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, fastPeriod,  slowPeriod,  functionDaysDiff, doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?,?)");

		List<String> inFileData = getFileData(DoubleBackAPOCorrelation.tabFile);

		conn.setAutoCommit(false);
		for (String in : inFileData) {
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement(
						"delete from apo_correlation" + " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackAPOCorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, DoubleBackAPOCorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackAPOCorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, DoubleBackAPOCorrelation.getFastPeriod(keyIn));
			stInsert.setInt(6, DoubleBackAPOCorrelation.getSlowPeriod(keyIn));
			stInsert.setInt(7, DoubleBackAPOCorrelation.getFunctionDaysDiff(keyIn));
			stInsert.setInt(8, DoubleBackAPOCorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(9, DoubleBackAPOCorrelation.getCorr(keyIn));
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

		List<String> inFileData = getFileData(DoubleBackDMICorrelation.tabFile);

		conn.setAutoCommit(false);
		for (String in : inFileData) {
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

	public static void dmimain_130(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into dmi_correlation_130"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, dmiPeriod,  functionDaysDiff, doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader("dmiDoubleTab130Days.txt");
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("set"))
				continue;
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement("delete from dmi_correlation_130"
						+ " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackDMICorrelation.getCloseSym(keyIn));
			if (DoubleBackDMICorrelation.getPricefunctionDaysDiff(keyIn) > 130)
				continue;

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

	public static void smimain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into sm_correlation"
				+ "(symbol,   toCloseDays, significantPlace, functionSymbol, hiLowPeriod, maPeriod, "
				+ "smSmoothPeriod, smSignalPeriod, functionDaysDiff, doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?,?,?,?)");

		List<String> inFileData = getFileData(DoubleBackSMICorrelation.tabFile);

		conn.setAutoCommit(false);
		for (String in : inFileData) {
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

	public static void smimain_130(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into sm_correlation_130"
				+ "(symbol,   toCloseDays, significantPlace, functionSymbol, hiLowPeriod, maPeriod, "
				+ "smSmoothPeriod, smSignalPeriod, functionDaysDiff, doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader("smiDoubleTaB130Days.txt");
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("set"))
				continue;
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement("delete from sm_correlation_130"
						+ " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}

			// 0:0.1659669430621573:aapl;1;sjnk;5;5;9;9;9;4;0.1659669430621573
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			// aapl;1;sjnk;5;5;9;9;9;4;0.1659669430621573
			// 1 2 3 4 5 6 7 8 9
			stInsert.setString(1, DoubleBackSMICorrelation.getCloseSym(keyIn));
			if (DoubleBackSMICorrelation.getPricefunctionDaysDiff(keyIn) > 130)
				continue;

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

		List<String> inFileData = getFileData(DoubleBackMACDCorrelation.tabFile);

		conn.setAutoCommit(false);
		for (String in : inFileData) {
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

	public static void macdmain_130(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into macd_correlation_130"
				+ "(symbol,   toCloseDays, significantPlace, functionSymbol, fastPeriod, slowPeriod, signalPeriod, functionDaysDiff,  doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader("macdDoubleTaB130Days.txt");
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("set"))
				continue;
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement("delete from macd_correlation_130"
						+ " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackMACDCorrelation.getCloseSym(keyIn));
			if (DoubleBackMACDCorrelation.getPricefunctionDaysDiff(keyIn) > 130)
				continue;
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

	public static void malinemain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into maline_correlation"
				+ "(symbol,   toCloseDays, significantPlace, functionSymbol, period, maType,   correlation)"
				+ " values (?,?,?,?,?,?,?)");

		List<String> inFileData = getFileData(MALineCorrelation.tabFile);

		conn.setAutoCommit(false);
		for (String in : inFileData) {
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement("delete from maline_correlation"
						+ " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, MALineCorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, MALineCorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, MALineCorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, MALineCorrelation.getMAPeriod(keyIn));
			stInsert.setString(6, MALineCorrelation.getMaType(keyIn));
			stInsert.setDouble(7, MALineCorrelation.getCorr(keyIn));
			stInsert.addBatch();
		}
		stInsert.executeBatch();
		stInsert.close();
		conn.commit();
		conn.close();
	}

	public static void psarmain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into psar_correlation"
				+ "(symbol,  toCloseDays, significantPlace, functionSymbol, acceleration, maximum,   functionDaysDiff,doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?,?)");

		List<String> inFileData = getFileData(DoubleBackPSARCorrelation.tabFile);

		conn.setAutoCommit(false);
		for (String in : inFileData) {
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement("delete from psar_correlation" + " where symbol = '"
						+ ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackPSARCorrelation.getCloseSym(keyIn));

			stInsert.setInt(2, DoubleBackPSARCorrelation.getFunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackPSARCorrelation.getFunctionSymbol(keyIn));
			stInsert.setDouble(5, DoubleBackPSARCorrelation.getAcceleration(keyIn));
			stInsert.setDouble(6, DoubleBackPSARCorrelation.getMaximum(keyIn));
			stInsert.setInt(7, DoubleBackPSARCorrelation.getDaysDiff(keyIn));
			stInsert.setInt(8, DoubleBackPSARCorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(9, DoubleBackPSARCorrelation.getCorr(keyIn));
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

		List<String> inFileData = getFileData(DoubleBackTSFCorrelation.tabFile);

		conn.setAutoCommit(false);
		for (String in : inFileData) {

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

	public static void tsfmain_130(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into tsf_correlation_130"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, tsfPeriod,  functionDaysDiff, doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader("TSFDoubleTaB130Days.txt");
		BufferedReader br = new BufferedReader(fr);
		String in = "";

		conn.setAutoCommit(false);
		while ((in = br.readLine()) != null) {
			if (in.contains("set"))
				continue;

			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement("delete from tsf_correlation_130"
						+ " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackTSFCorrelation.getCloseSym(keyIn));
			if (DoubleBackTSFCorrelation.getPricefunctionDaysDiff(keyIn) > 130)
				continue;
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

	public static void mamain(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		LogManager.getLogManager().reset();

		final PreparedStatement stInsert = conn.prepareStatement("insert into ma_correlation"
				+ "(symbol, toCloseDays, significantPlace, functionSymbol, period, matype, functionDaysDiff, doubleBack, correlation)"
				+ " values (?,?,?,?,?,?,?,?,?)");

		FileReader fr = new FileReader(DoubleBackMACorrelation.tabFile);
		List<String> inFileData = getFileData(DoubleBackMACorrelation.tabFile);

		conn.setAutoCommit(false);
		for (String in : inFileData) {
			if (in.contains("_")) {
				String ins[] = in.split("_");
				PreparedStatement stDelete = conn.prepareStatement(
						"delete from ma_correlation" + " where symbol = '" + ins[0] + "' and toCloseDays = " + ins[1]);
				stDelete.execute();
				continue;
			}
			String inColon[] = in.split(":");
			String keyIn = inColon[2];
			stInsert.setString(1, DoubleBackMACorrelation.getCloseSym(keyIn));
			stInsert.setInt(2, DoubleBackMACorrelation.getPricefunctionDaysDiff(keyIn));
			stInsert.setInt(3, Integer.parseInt(inColon[0]));
			stInsert.setString(4, DoubleBackMACorrelation.getfunctionSymbol(keyIn));
			stInsert.setInt(5, DoubleBackMACorrelation.getPeriod(keyIn));
			stInsert.setString(6, DoubleBackMACorrelation.getMAType(keyIn));
			stInsert.setInt(7, DoubleBackMACorrelation.getMAfunctionDaysDiff(keyIn));
			stInsert.setInt(8, DoubleBackMACorrelation.getDoubleBack(keyIn));
			stInsert.setDouble(9, DoubleBackMACorrelation.getCorr(keyIn));
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
