package correlation;

import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import util.Averager;
import utils.ScaleFrom0to1;

public class Correlation {

	public static void main(String args[]) {

		ArrayList<Double> x = new ArrayList<>();
		ArrayList<Double> y = new ArrayList<>();
		Random rand = new Random();
		for (int i = 0; i < 1e2; i++) {
			double d = rand.nextDouble();
			x.add(d);
			d = rand.nextDouble();
			y.add(d);
		}
		System.out.println(mcverrysCorrelation(x, y));
	}

	public static double pearsonsCorrelation(ArrayList<Double> xs, ArrayList<Double> ys) {
		// TODO: check here that arrays are not null, of the same length etc

		PearsonsCorrelation pc = new PearsonsCorrelation();

		double[] xa = new double[xs.size()];
		double[] ya = new double[ys.size()];

		double sx = 0.0;
		double sy = 0.0;
		double sxx = 0.0;
		double syy = 0.0;
		double sxy = 0.0;

		int n = xs.size();

		for (int i = 0; i < n; ++i) {
			double x = xs.get(i);
			xa[i] = x;
			double y = ys.get(i);
			ya[i] = y;

			sx += x;
			sy += y;
			sxx += x * x;
			syy += y * y;
			sxy += x * y;
		}

		// covariation
		double cov = sxy / n - sx * sy / n / n;
		// standard error of x
		double sigmax = Math.sqrt(sxx / n - sx * sx / n / n);
		// standard error of y
		double sigmay = Math.sqrt(syy / n - sy * sy / n / n);

		// correlation is just a normalized covariation

		return cov / sigmax / sigmay;
	}

	public static double mcverrysCorrelation(ArrayList<Double> xs, ArrayList<Double> ys) {

		ArrayList<Double> nx = ScaleFrom0to1.nominalize(xs);
		ArrayList<Double> ny = ScaleFrom0to1.nominalize(ys);

		int n = xs.size();

		Averager coefAverager = new Averager();
		for (int i = 0; i < n; ++i) {
			coefAverager.add((nx.get(i) - .5) - (ny.get(i) - .5));

		}
		if (coefAverager.get() == 0)
			return 0;

		return 1 / coefAverager.get();
	}
}
