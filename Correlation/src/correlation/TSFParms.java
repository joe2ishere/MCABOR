package correlation;

import java.util.Set;
import java.util.TreeMap;

public class TSFParms implements AttributeParm {
	public record TSFSymbolParm(Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] tsfs) {

	}

	TreeMap<String, TSFSymbolParm> TSFPMap;

	public TSFParms() {
		TSFPMap = new TreeMap<String, TSFSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return TSFPMap.keySet();
	}

	public void addSymbol(String sym, Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] tsfs) {
		TSFPMap.put(sym, new TSFSymbolParm(functionDaysDiff, doubleBacks, dateIndex, tsfs));

	}

	@Override
	public Integer getDaysDiff(String sym) {
		return TSFPMap.get(sym).functionDaysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return TSFPMap.get(sym).doubleBacks;
	}

	public double[] gettsfs(String sym) {
		return TSFPMap.get(sym).tsfs;
	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return TSFPMap.get(sym).dateIndex;
	}
}
