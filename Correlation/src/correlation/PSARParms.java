package correlation;

import java.util.Set;
import java.util.TreeMap;

public class PSARParms implements AttributeParm {
	public record PSARSymbolParm(Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] psar) {

	}

	TreeMap<String, PSARSymbolParm> PSARPMap;

	public PSARParms() {
		PSARPMap = new TreeMap<String, PSARSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return PSARPMap.keySet();
	}

	public void addSymbol(String sym, Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] psar) {
		PSARPMap.put(sym, new PSARSymbolParm(functionDaysDiff, doubleBacks, dateIndex, psar));
	}

	@Override
	public Integer getDaysDiff(String sym) {
		return PSARPMap.get(sym).functionDaysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return PSARPMap.get(sym).doubleBacks;
	}

	public double[] getPSAR(String sym) {
		return PSARPMap.get(sym).psar;
	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return PSARPMap.get(sym).dateIndex;
	}

}
