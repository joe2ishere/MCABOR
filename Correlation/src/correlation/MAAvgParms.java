package correlation;

import java.util.Set;
import java.util.TreeMap;

public class MAAvgParms implements AttributeParm {
	public class MAAvgSymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		String[] dates;
		int lastDateStart;
		MaLineParmToPass mali;
	}

	TreeMap<String, MAAvgSymbolParm> MAAvgPMap;

	public MAAvgParms() {
		MAAvgPMap = new TreeMap<String, MAAvgSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return MAAvgPMap.keySet();
	}

	@Override
	public void addSymbol(String sym) {
		MAAvgPMap.put(sym, new MAAvgSymbolParm());
		MAAvgPMap.get(sym).lastDateStart = 0;
	}

	@Override
	public Integer getDaysDiff(String sym) {
		return MAAvgPMap.get(sym).functionDaysDiff;
	}

	@Override
	public void setDaysDiff(String sym, Integer daysDiff) {
		MAAvgPMap.get(sym).functionDaysDiff = daysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return MAAvgPMap.get(sym).doubleBacks;
	}

	@Override
	public void setDoubleBacks(String sym, Integer doubleBacks) {
		MAAvgPMap.get(sym).doubleBacks = doubleBacks;
	}

	public MaLineParmToPass getMALI(String sym) {
		return MAAvgPMap.get(sym).mali;
	}

	public void setMALI(String sym, MaLineParmToPass mali) {
		MAAvgPMap.get(sym).mali = mali;
	}

	@Override
	public String[] getAttrDates(String sym) {
		return MAAvgPMap.get(sym).dates;
	}

	@Override
	public void setAttrDates(String sym, String dates[]) {
		MAAvgPMap.get(sym).dates = dates;
	}

	@Override
	public int getLastDateStart(String sym) {
		return MAAvgPMap.get(sym).lastDateStart;
	}

	@Override
	public void setLastDateStart(String sym, int start) {
		MAAvgPMap.get(sym).lastDateStart = start;
	}

}
