package correlation;

import java.util.Set;
import java.util.TreeMap;

public class APOParms implements AttributeParm {
	public class APOSymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		TreeMap<String, Integer> dateIndex;
		double[] APOs;
	}

	TreeMap<String, APOSymbolParm> APOPMap;

	public APOParms() {
		APOPMap = new TreeMap<String, APOSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return APOPMap.keySet();
	}

	@Override
	public void addSymbol(String sym) {
		APOPMap.put(sym, new APOSymbolParm());

	}

	@Override
	public Integer getDaysDiff(String sym) {
		return APOPMap.get(sym).functionDaysDiff;
	}

	@Override
	public void setDaysDiff(String sym, Integer daysDiff) {
		APOPMap.get(sym).functionDaysDiff = daysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return APOPMap.get(sym).doubleBacks;
	}

	@Override
	public void setDoubleBacks(String sym, Integer doubleBacks) {
		APOPMap.get(sym).doubleBacks = doubleBacks;
	}

	public double[] getAPOs(String sym) {
		return APOPMap.get(sym).APOs;
	}

	public void setAPOs(String sym, double[] APOs) {
		APOPMap.get(sym).APOs = APOs;
	}

	@Override
	public void setDateIndex(String sym, TreeMap<String, Integer> dateIndex) {
		APOPMap.get(sym).dateIndex = dateIndex;

	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return APOPMap.get(sym).dateIndex;
	}

}
