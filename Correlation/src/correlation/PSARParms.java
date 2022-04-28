package correlation;

import java.util.Set;
import java.util.TreeMap;

public class PSARParms implements AttributeParm {
	public class PSARSymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		TreeMap<String, Integer> dateIndex;

		double[] psar;

	}

	TreeMap<String, PSARSymbolParm> PSARPMap;

	public PSARParms() {
		PSARPMap = new TreeMap<String, PSARSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return PSARPMap.keySet();
	}

	@Override
	public void addSymbol(String sym) {
		PSARPMap.put(sym, new PSARSymbolParm());
	}

	@Override
	public Integer getDaysDiff(String sym) {
		return PSARPMap.get(sym).functionDaysDiff;
	}

	@Override
	public void setDaysDiff(String sym, Integer daysDiff) {
		PSARPMap.get(sym).functionDaysDiff = daysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return PSARPMap.get(sym).doubleBacks;
	}

	@Override
	public void setDoubleBacks(String sym, Integer doubleBacks) {
		PSARPMap.get(sym).doubleBacks = doubleBacks;
	}

	public double[] getPSAR(String sym) {
		return PSARPMap.get(sym).psar;
	}

	public void setPSAR(String sym, double[] psar) {
		PSARPMap.get(sym).psar = psar;
	}

	@Override
	public void setDateIndex(String sym, TreeMap<String, Integer> dateIndex) {
		PSARPMap.get(sym).dateIndex = dateIndex;

	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return PSARPMap.get(sym).dateIndex;
	}

}
