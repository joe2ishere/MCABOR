package correlation;

import java.util.Set;
import java.util.TreeMap;

public class MAAvgParms implements AttributeParm {
	public class MAAvgSymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		TreeMap<String, Integer> dateIndex;
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
	public TreeMap<String, Integer> getDateIndex(String sym) {
		return MAAvgPMap.get(sym).dateIndex;
	}

	@Override
	public void setDateIndex(String sym, TreeMap<String, Integer> dateIndex) {
		MAAvgPMap.get(sym).dateIndex = dateIndex;

	}

}
