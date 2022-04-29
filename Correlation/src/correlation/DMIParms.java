package correlation;

import java.util.Set;
import java.util.TreeMap;

public class DMIParms implements AttributeParm {
	public record DMISymbolParm(Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] DMIs) {

	}

	TreeMap<String, DMISymbolParm> DMIPMap;

	public DMIParms() {
		DMIPMap = new TreeMap<String, DMISymbolParm>();
	}

	public void addSymbol(String sym, Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] DMIs) {
		DMIPMap.put(sym, new DMISymbolParm(functionDaysDiff, doubleBacks, dateIndex, DMIs));

	}

	@Override
	public Set<String> keySet() {
		return DMIPMap.keySet();
	}

	@Override
	public Integer getDaysDiff(String sym) {
		return DMIPMap.get(sym).functionDaysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return DMIPMap.get(sym).doubleBacks;
	}

	public double[] getDMIs(String sym) {
		return DMIPMap.get(sym).DMIs;
	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return DMIPMap.get(sym).dateIndex;
	}

}
