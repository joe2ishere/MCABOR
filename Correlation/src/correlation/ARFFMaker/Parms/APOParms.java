package correlation.ARFFMaker.Parms;

import java.util.Set;
import java.util.TreeMap;

public class APOParms implements AttributeParm {
	public record APOSymbolParm(Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] APOs) {

	}

	TreeMap<String, APOSymbolParm> APOPMap;

	public APOParms() {
		APOPMap = new TreeMap<String, APOSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return APOPMap.keySet();
	}

	public void addSymbol(String sym, Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] APOs) {
		APOPMap.put(sym, new APOSymbolParm(functionDaysDiff, doubleBacks, dateIndex, APOs));

	}

	@Override
	public Integer getDaysDiff(String sym) {
		return APOPMap.get(sym).functionDaysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {

		return APOPMap.get(sym).doubleBacks;
	}

	public double[] getAPOs(String sym) {
		return APOPMap.get(sym).APOs;
	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return APOPMap.get(sym).dateIndex;
	}

}
