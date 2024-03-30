package org.mcabor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class Cointegration {

	public static double cointegration(Double[] first, Double[] second) throws BadCointegrationParm {
		return cointegration(new ArrayList<Double>(Arrays.asList(first)), new ArrayList<Double>(Arrays.asList(second)));
	}

	public static double cointegration(ArrayList<Double> first, ArrayList<Double> second) throws BadCointegrationParm {

		if (first == null)
			throw new BadCointegrationParm("first vector is empty");
		if (second == null)
			throw new BadCointegrationParm("second vector is empty");
		if (first.size() != second.size())
			throw new BadCointegrationParm("array lengths must match");
		if (first.size() == 0)
			throw new BadCointegrationParm("array length (first vector) must be greater than 0");
		double sum = 0.0;
		Double firstMin = first.stream().mapToDouble(d -> d).min()
				.orElseThrow(() -> new BadCointegrationParm("Can't find minimum in first vector"));
		Double firstMax = first.stream().mapToDouble(d -> d).max()
				.orElseThrow(() -> new BadCointegrationParm("Can't find maximum in first vector"));
		if (firstMin.doubleValue() == firstMax.doubleValue())
			throw new BadCointegrationParm("array range (first vector) must not be 0");
		Double secondMin = second.stream().mapToDouble(d -> d).min()
				.orElseThrow(() -> new BadCointegrationParm("Can't find minimum in second vector"));
		Double secondMax = second.stream().mapToDouble(d -> d).max()
				.orElseThrow(() -> new BadCointegrationParm("Can't find maximum in second vector"));
		if (secondMin.doubleValue() == secondMax.doubleValue())
			throw new BadCointegrationParm("array range (second vector) must not be 0");
		double firstDivide = firstMax - firstMin;
		double secondDivide = secondMax - secondMin;
		Iterator<Double> firstIter = first.iterator();
		Iterator<Double> secondIter = second.iterator();
		while (firstIter.hasNext()) {
			double firstDiff = (firstMax - firstIter.next()) / firstDivide;
			double secondDiff = (secondMax - secondIter.next()) / secondDivide;
			double absDiff = Math.abs(firstDiff - secondDiff);
			sum += absDiff;
			if (Double.isNaN(sum))
				throw new BadCointegrationParm("why NAN?");
		}
		if (Double.isNaN(sum / first.size()))
			throw new BadCointegrationParm("why NAN?");
		return sum / first.size();
	}

}
