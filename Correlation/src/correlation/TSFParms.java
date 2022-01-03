package correlation;

import java.util.Set;
import java.util.TreeMap;

public class TSFParms implements AttributeParm {
	public class TSFSymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		String[] dates;
		int lastDateStart;
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
		TSFPMap.get(sym).lastDateStart = 0;
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
	public String[] getAttrDates(String sym) {
		return TSFPMap.get(sym).dates;
	}

	@Override
	public void setAttrDates(String sym, String dates[]) {
		TSFPMap.get(sym).dates = dates;
	}

	@Override
	public int getLastDateStart(String sym) {
		return TSFPMap.get(sym).lastDateStart;
	}

	@Override
	public void setLastDateStart(String sym, int start) {
		TSFPMap.get(sym).lastDateStart = start;
	}

}
