package correlation.ARFFMaker.Parms;

import java.util.Set;
import java.util.TreeMap;

public class MAAvgParms implements AttributeParm {
	public record MAAvgSymbolParm(Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			MaLineParmToPass mali) {

	}

	TreeMap<String, MAAvgSymbolParm> MAAvgPMap;

	public MAAvgParms() {
		MAAvgPMap = new TreeMap<String, MAAvgSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return MAAvgPMap.keySet();
	}

	public void addSymbol(String sym, Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			MaLineParmToPass mali) {
		MAAvgPMap.put(sym, new MAAvgSymbolParm(functionDaysDiff, doubleBacks, dateIndex, mali));

	}

	@Override
	public Integer getDaysDiff(String sym) {
		return MAAvgPMap.get(sym).functionDaysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return MAAvgPMap.get(sym).doubleBacks;
	}

	public MaLineParmToPass getMALI(String sym) {
		return MAAvgPMap.get(sym).mali;
	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {
		return MAAvgPMap.get(sym).dateIndex;
	}

}
