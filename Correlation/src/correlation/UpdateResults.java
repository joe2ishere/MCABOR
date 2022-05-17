package correlation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import util.getDatabaseConnection;

public class UpdateResults {
	public static void main(String[] args) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();
		File filedir = new File("c:/users/joe/correlationOutput/");
		File files[] = filedir.listFiles();
		for (File file : files) {

			String name = file.getName();
			if (name.contains("resultsForFunctionTest_") == false)
				continue;
			if (name.contains("Debug") == true)
				continue;
			if (name.endsWith(".csv") == false)
				continue;
			if (name.compareTo("resultsForFunctionTest_2022-02-01") < 0)
				continue;
			System.out.println(name);
			updateResultsTable(conn, file, false);
		}

	}

	public static void updateResultsTable(Connection conn, File resultsForDBFile2, boolean debugging) throws Exception {

		String debugAppend = debugging ? "_debug" : "";

		BufferedReader br = new BufferedReader(new FileReader(resultsForDBFile2));
		String in;
		conn.setAutoCommit(false);
		PreparedStatement containsStatement = conn.prepareStatement("select count(*) from correlationfunctionresults"
				+ debugAppend + " where symbol = ? and function = ? and mktDate = ? and daysout = ? ");
		PreparedStatement insertFunctionResults = conn.prepareStatement("insert into correlationfunctionresults"
				+ debugAppend + " (symbol, function, mktDate, daysOut, guess) " + " values(?,?,?,?,?) ");
		PreparedStatement updateFunctionResults = conn
				.prepareStatement("update correlationfunctionresults" + debugAppend + " set guess = (guess + ?) / 2  "
						+ " where symbol = ? and function = ? and mktDate = ? and daysout = ? ");

		while ((in = br.readLine()) != null) {

			String ins[] = in.split(";");
			if (ins.length < 4)
				continue;
			containsStatement.setString(1, ins[0]);
			containsStatement.setString(2, ins[1]);
			containsStatement.setString(3, ins[2]);
			containsStatement.setInt(4, Integer.parseInt(ins[3]));
			ResultSet rsContains = containsStatement.executeQuery();
			if (rsContains.next()) {
				int cnt = rsContains.getInt(1);
				if (cnt > 0) {
					updateFunctionResults.setDouble(1, Double.parseDouble(ins[4]));
					updateFunctionResults.setString(2, ins[0]);
					updateFunctionResults.setString(3, ins[1]);
					updateFunctionResults.setString(4, ins[2]);
					updateFunctionResults.setInt(5, Integer.parseInt(ins[3]));
					updateFunctionResults.execute();
					continue;

				}
			}

			insertFunctionResults.setString(1, ins[0]);
			insertFunctionResults.setString(2, ins[1]);
			insertFunctionResults.setString(3, ins[2]);
			insertFunctionResults.setInt(4, Integer.parseInt(ins[3]));
			insertFunctionResults.setDouble(5, Double.parseDouble(ins[4]));
			insertFunctionResults.execute();
		}
		conn.commit();
		conn.setAutoCommit(true);
		br.close();
	}

}
