package correlation;

import java.util.Set;
import java.util.TreeMap;

public class DMIParms implements AttributeParm {
	public class DMISymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		String[] dates;
		int lastDateStart;
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
		DMIPMap.get(sym).lastDateStart = 0;
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
	public String[] getAttrDates(String sym) {
		return DMIPMap.get(sym).dates;
	}

	@Override
	public void setAttrDates(String sym, String dates[]) {
		DMIPMap.get(sym).dates = dates;
	}

	@Override
	public int getLastDateStart(String sym) {
		return DMIPMap.get(sym).lastDateStart;
	}

	@Override
	public void setLastDateStart(String sym, int start) {
		DMIPMap.get(sym).lastDateStart = start;
	}

}
