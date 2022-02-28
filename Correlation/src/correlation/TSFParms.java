package correlation;

import java.util.Set;
import java.util.TreeMap;

public class TSFParms implements AttributeParm {
	public class TSFSymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		TreeMap<String, Integer> dateIndex;
		double[] tsfs;
	}

	TreeMap<String, TSFSymbolParm> TSFPMap;

	public TSFParms() {
		TSFPMap = new TreeMap<String, TSFSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return TSFPMap.keySet();
	}

	@Override
	public void addSymbol(String sym) {
		TSFPMap.put(sym, new TSFSymbolParm());

	}

	@Override
	public Integer getDaysDiff(String sym) {
		return TSFPMap.get(sym).functionDaysDiff;
	}

	@Override
	public void setDaysDiff(String sym, Integer daysDiff) {
		TSFPMap.get(sym).functionDaysDiff = daysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return TSFPMap.get(sym).doubleBacks;
	}

	@Override
	public void setDoubleBacks(String sym, Integer doubleBacks) {
		TSFPMap.get(sym).doubleBacks = doubleBacks;
	}

	public double[] gettsfs(String sym) {
		return TSFPMap.get(sym).tsfs;
	}

	public void settsfs(String sym, double[] tsfs) {
		TSFPMap.get(sym).tsfs = tsfs;
	}

	@Override
	public void setDateIndex(String sym, TreeMap<String, Integer> dateIndex) {
		TSFPMap.get(sym).dateIndex = dateIndex;

	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return TSFPMap.get(sym).dateIndex;
	}
}
