package correlation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import bands.DeltaBands;
import util.Averager;
import utils.getDatabaseConnection;

public class SectorGraph {

	public static void main(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();

		PreparedStatement ps = conn.prepareStatement("select distinct category from etfcategory");
		ArrayList<String> sectors = new ArrayList<>();
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			if (rs.getString(1).trim().length() < 0)
				continue;
			sectors.add(rs.getString(1).trim());
		}

		ps = conn.prepareStatement("select distinct mktdate from correlation30days order by mktdate desc limit 1");
		rs = ps.executeQuery();
		rs.next();
		String lastMktDate = rs.getString(1);
		System.out.println(lastMktDate);

		ps = conn.prepareStatement("select symbol from etfcategory where category = ?");
		ps.setString(1, sectors.get(11));
		rs = ps.executeQuery();
		System.out.println(sectors.get(11));

		ArrayList<Averager> sectorAverages = new ArrayList<>();

		while (rs.next()) {
			String sym = rs.getString(1);
			GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);
			double[] closes = gsd.inClose;
			for (int goback = 40; goback >= 1; goback--) {
				DeltaBands db = new DeltaBands(closes, goback);
				if (sectorAverages.size() < 40) {
					Averager avg = new Averager();
					sectorAverages.add(avg);
				}
				Averager avg = sectorAverages.get(40 - goback);
				int len = closes.length - 1;
				double got = db.getBandCount(len - goback, goback, closes);
				avg.add(got);

			}

			if (sectorAverages.size() == 40) {
				Averager avg = new Averager();
				sectorAverages.add(avg);
				avg.add(4.5);
			}
			PreparedStatement psSym = conn
					.prepareStatement("select guesses from correlation30days where symbol = ? and mktdate = ?");
			psSym.setString(1, sym);
			psSym.setString(2, lastMktDate);
			ResultSet rsSym = psSym.executeQuery();
			rsSym.next();
			String guesses = rsSym.getString(1);
			String guessSplit[] = guesses.split(";");
			for (int gi = 1; gi <= 30; gi++) {
				String guess = guessSplit[gi - 1];
				double dg = Double.parseDouble(guess);
				if (sectorAverages.size() <= 40 + gi) {
					Averager avg = new Averager();
					sectorAverages.add(avg);
				}
				Averager avg = sectorAverages.get(40 + gi);
				avg.add(dg);
			}

		}

		for (Averager avg : sectorAverages)
			System.out.print(avg.get() + ";");
		System.out.println();
	}

}
