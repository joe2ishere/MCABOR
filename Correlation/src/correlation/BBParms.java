package correlation;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;

public class BBParms implements AttributeParm {
	public class BBSymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		ArrayList<double[]> bbs;
		String[] dates;
		int lastDateStart;
	}

	TreeMap<String, BBSymbolParm> BBPMap;

	public BBParms() {
		BBPMap = new TreeMap<String, BBSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return BBPMap.keySet();
	}

	@Override
	public void addSymbol(String sym) {
		BBPMap.put(sym, new BBSymbolParm());
		BBPMap.get(sym).lastDateStart = 0;
	}

	@Override
	public Integer getDaysDiff(String sym) {
		return BBPMap.get(sym).functionDaysDiff;
	}

	@Override
	public void setDaysDiff(String sym, Integer daysDiff) {
		BBPMap.get(sym).functionDaysDiff = daysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return BBPMap.get(sym).doubleBacks;
	}

	@Override
	public void setDoubleBacks(String sym, Integer doubleBacks) {
		BBPMap.get(sym).doubleBacks = doubleBacks;
	}

	public ArrayList<double[]> getBBs(String sym) {
		return BBPMap.get(sym).bbs;
	}

	public void setBBs(String sym, ArrayList<double[]> bbs) {
		BBPMap.get(sym).bbs = bbs;
	}

	@Override
	public String[] getAttrDates(String sym) {
		return BBPMap.get(sym).dates;
	}

	@Override
	public void setAttrDates(String sym, String dates[]) {
		BBPMap.get(sym).dates = dates;
	}

	@Override
	public int getLastDateStart(String sym) {
		return BBPMap.get(sym).lastDateStart;
	}

	@Override
	public void setLastDateStart(String sym, int start) {
		BBPMap.get(sym).lastDateStart = start;
	}

}