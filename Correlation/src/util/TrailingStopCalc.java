package util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DecimalFormat;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;

import utils.StandardDeviation;

public class TrailingStopCalc {

	public static void main(String[] args) throws Exception {
		int period = 20;
		System.out.println("Period is set to " + period + " days.");
		System.out.println("enter symbol");
		DecimalFormat df = new DecimalFormat("#.000");
		DecimalFormat df2 = new DecimalFormat("#.00");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String sym = br.readLine();
		GetETFDataUsingSQL getf = GetETFDataUsingSQL.getInstance(sym);
		StandardDeviation stddev = new StandardDeviation();
		for (int i = period; i < getf.inClose.length - 1; i++) {
			double max = getf.inClose[i + 1];
			double min = max;
			for (int ii = 0; ii < period; ii++) {
				max = Math.max(getf.inClose[i - ii], max);
				min = Math.min(getf.inClose[i - ii], min);
			}
			if (max > getf.inClose[i + 1]) {
				stddev.enter((max / getf.inClose[i + 1]) - 1);
				// System.out.println(max / getf.inClose[i + 1]);
			}
			if (min < getf.inClose[i + 1]) {
				stddev.enter((getf.inClose[i + 1] / min) - 1);
				// System.out.println(min / getf.inClose[i + 1]);
			}

		}
		double mean = stddev.getMean();
		System.out.println("average % change is " + df.format(stddev.getMean() * 100) + "; std dev is "
				+ df.format(stddev.getStandardDeviation() * 100));
		System.out.println("type: std dev 0; 1; 1.5; 2; 2.5; 3 ");

		System.out.println(
				"Percentage: " + df.format(mean * 100) + ";" + df.format((mean + stddev.getStandardDeviation()) * 100)
						+ ";" + df.format((mean + (stddev.getStandardDeviation() * 1.5)) * 100) + ";"
						+ df.format((mean + (stddev.getStandardDeviation() * 2)) * 100)
						+ df.format((mean + (stddev.getStandardDeviation() * 2.5)) * 100) + ";"
						+ df.format((mean + (stddev.getStandardDeviation() * 3)) * 100));
		System.out.println("Dollar: " + df2.format(getf.inClose[getf.inClose.length - 1] * mean) + ";"
				+ df2.format(getf.inClose[getf.inClose.length - 1] * (mean + stddev.getStandardDeviation())) + ";"
				+ df2.format(getf.inClose[getf.inClose.length - 1] * (mean + (stddev.getStandardDeviation() * 1.5)))
				+ ";" + df2.format(getf.inClose[getf.inClose.length - 1] * (mean + (stddev.getStandardDeviation() * 2)))
				+ df2.format(getf.inClose[getf.inClose.length - 1] * (mean + (stddev.getStandardDeviation() * 2.5)))
				+ ";"
				+ df2.format(getf.inClose[getf.inClose.length - 1] * (mean + (stddev.getStandardDeviation() * 3))));

	}

}
