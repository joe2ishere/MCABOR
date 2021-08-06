package correlation.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestLengthOfReport {

	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection(
				"jdbc:mysql://mcverryreport.com/mcverr5_reports?user=mcverr5_sqlDwnld&password=y(Y}^mOZ+bOA");
		PreparedStatement ps = conn.prepareStatement("select dateOfReport, currentReport from forecastReport102030");
		ResultSet rs = ps.executeQuery();
		Pattern pat = Pattern.compile("(<tr>(.*?)</tr>)+");
		while (rs.next()) {
			Matcher mat = pat.matcher(rs.getString("currentReport"));
			if (mat.find()) {
				System.out.println("found " + mat.groupCount());
				System.out.println(mat.group(1));
				System.out.println(mat.group(2));
			}

		}
		conn.close();

	}

}
