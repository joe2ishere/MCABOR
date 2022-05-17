package correlation.ARFFMaker.Parms;

import java.util.Set;
import java.util.TreeMap;

public class ATRParms implements AttributeParm {
	public record ATRSymbolParm(Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] ATRs) {

	}

	TreeMap<String, ATRSymbolParm> ATRPMap;

	public ATRParms() {
		ATRPMap = new TreeMap<String, ATRSymbolParm>();
	}

	public void addSymbol(String sym, Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] ATRs) {
		ATRPMap.put(sym, new ATRSymbolParm(functionDaysDiff, doubleBacks, dateIndex, ATRs));

	}

	@Override
	public Set<String> keySet() {
		return ATRPMap.keySet();
	}

	@Override
	public Integer getDaysDiff(String sym) {
		return ATRPMap.get(sym).functionDaysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return ATRPMap.get(sym).doubleBacks;
	}

	public double[] getATRs(String sym) {
		return ATRPMap.get(sym).ATRs;
	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return ATRPMap.get(sym).dateIndex;
	}

}
