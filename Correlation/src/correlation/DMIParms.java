package correlation;

import java.util.Set;
import java.util.TreeMap;

public class DMIParms implements AttributeParm {
	public class DMISymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		TreeMap<String, Integer> dateIndex;
		double[] DMIs;
	}

	TreeMap<String, DMISymbolParm> DMIPMap;

	public DMIParms() {
		DMIPMap = new TreeMap<String, DMISymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return DMIPMap.keySet();
	}

	@Override
	public void addSymbol(String sym) {
		DMIPMap.put(sym, new DMISymbolParm());

	}

	@Override
	public Integer getDaysDiff(String sym) {
		return DMIPMap.get(sym).functionDaysDiff;
	}

	@Override
	public void setDaysDiff(String sym, Integer daysDiff) {
		DMIPMap.get(sym).functionDaysDiff = daysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return DMIPMap.get(sym).doubleBacks;
	}

	@Override
	public void setDoubleBacks(String sym, Integer doubleBacks) {
		DMIPMap.get(sym).doubleBacks = doubleBacks;
	}

	public double[] getDMIs(String sym) {
		return DMIPMap.get(sym).DMIs;
	}

	public void setDMIs(String sym, double[] DMIs) {
		DMIPMap.get(sym).DMIs = DMIs;
	}

	@Override
	public void setDateIndex(String sym, TreeMap<String, Integer> dateIndex) {
		DMIPMap.get(sym).dateIndex = dateIndex;

	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return DMIPMap.get(sym).dateIndex;
	}

}
