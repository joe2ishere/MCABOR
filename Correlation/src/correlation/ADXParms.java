package correlation;

import java.util.Set;
import java.util.TreeMap;

public class ADXParms implements AttributeParm {
	public class ADXSymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		double[] adxs;
		String[] dates;
		int lastDateStart;
	}

	TreeMap<String, ADXSymbolParm> ADXPMap;

	public ADXParms() {
		ADXPMap = new TreeMap<String, ADXSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return ADXPMap.keySet();
	}

	@Override
	public void addSymbol(String sym) {
		ADXPMap.put(sym, new ADXSymbolParm());
		ADXPMap.get(sym).lastDateStart = 0;
	}

	@Override
	public Integer getDaysDiff(String sym) {
		return ADXPMap.get(sym).functionDaysDiff;
	}

	@Override
	public void setDaysDiff(String sym, Integer daysDiff) {
		ADXPMap.get(sym).functionDaysDiff = daysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return ADXPMap.get(sym).doubleBacks;
	}

	@Override
	public void setDoubleBacks(String sym, Integer doubleBacks) {
		ADXPMap.get(sym).doubleBacks = doubleBacks;
	}

	public double[] getADXS(String sym) {
		return ADXPMap.get(sym).adxs;
	}

	public void setADXS(String sym, double[] adxs) {
		ADXPMap.get(sym).adxs = adxs;
	}

	@Override
	public String[] getAttrDates(String sym) {
		return ADXPMap.get(sym).dates;
	}

	@Override
	public void setAttrDates(String sym, String dates[]) {
		ADXPMap.get(sym).dates = dates;
	}

	@Override
	public int getLastDateStart(String sym) {
		return ADXPMap.get(sym).lastDateStart;
	}

	@Override
	public void setLastDateStart(String sym, int start) {
		ADXPMap.get(sym).lastDateStart = start;
	}

}