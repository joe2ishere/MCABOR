package correlation;

import java.io.File;
import java.io.PrintWriter;
import java.util.Random;
import java.util.TreeMap;

public class ReportForStockTwits {

	static String[] messageArray = { "   $%s is a %s over the %s estimated by my Machine Learning models",
			"   My A.I. models have $%s as a %s for the %s", "   $%s as a %s for the %s using A.I. models",
			"   A.I. models say $%s is a %s over the %s", "   $%s looking like a %s for the %s",
			"   $%s could be a %s for the %s, a forecast by my A.I. models",
			"   My Machine Learning models calling for $%s as a %s for the %s",
			"   Using A.I. models I get $%s as a %s for the %s", "   I have $%s as a %s for the %s using A.I. models",
			"   $%s estimated to be a %s for the %s using A.I. models",
			"   I'm getting $%s as a %s for the %s estimated by my A.I. models",
			"   Estimates by Machine Learning models have $%s as a %s over the %s" };

	static String[] seeMoreAt = { "; additional estimates-> http://www.mcverryreport.com/forecast",
			"; other estimates at http://www.mcverryreport.com/forecast", "; see http://www.mcverryreport.com/forecast",
			"; reference http://www.mcverryreport.com/forecast", "; details at http://www.mcverryreport.com/forecast" };

	static Random rand = new Random();

	public void get(TreeMap<String, String> day5, TreeMap<String, String> day10) throws Exception {

		PrintWriter pwStockTwits = new PrintWriter(System.getProperty("user.home") + File.separator + "Documents"
				+ File.separator + "stockReports" + File.separator + "stocktwitsReport.txt");

		int msgcnt = (int) Math.round(messageArray.length * Math.random());
		if (msgcnt >= messageArray.length)
			msgcnt = 0;
		int morecnt = 0;

		for (String key : day5.keySet()) {
			boolean dorw = rand.nextBoolean();
			boolean dorNumber = rand.nextBoolean();
			if (day5.get(key).startsWith("B") | day5.get(key).startsWith("S")) {
				pwStockTwits.println(String.format(messageArray[msgcnt], key.toUpperCase(), day5.get(key),
						(dorw ? "next " + (dorNumber ? "5" : "five") + " days" : "next week")) + seeMoreAt[morecnt]
						+ "\n");
				msgcnt++;
				morecnt++;
				if (msgcnt == messageArray.length)
					msgcnt = 0;
				if (morecnt == seeMoreAt.length)
					morecnt = 0;

			}
		}

		for (String key : day10.keySet()) {
			boolean dorw = rand.nextBoolean();
			boolean dorNumber = rand.nextBoolean();

			if (day10.get(key).startsWith("B") | day10.get(key).startsWith("S")) {
				pwStockTwits
						.println(
								String.format(messageArray[msgcnt], key.toUpperCase(), day10.get(key),
										(dorw ? "next " + (dorNumber ? "10" : "ten") + " days"
												: "next " + (dorNumber ? "2" : "two") + " weeks"))
										+ seeMoreAt[morecnt] + "\n");
				msgcnt++;
				morecnt++;
				if (msgcnt == messageArray.length)
					msgcnt = 0;
				if (morecnt == seeMoreAt.length)
					morecnt = 0;

			}
		}

		pwStockTwits.flush();
		pwStockTwits.close();

	}

}
