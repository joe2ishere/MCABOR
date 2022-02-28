package correlation;

import java.util.Set;
import java.util.TreeMap;

public class MAParms implements AttributeParm {
	public class MASymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		TreeMap<String, Integer> dateIndex;
		double[] MAs;
	}

	TreeMap<String, MASymbolParm> MAPMap;

	public MAParms() {
		MAPMap = new TreeMap<String, MASymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return MAPMap.keySet();
	}

	@Override
	public void addSymbol(String sym) {
		MAPMap.put(sym, new MASymbolParm());

	}

	@Override
	public Integer getDaysDiff(String sym) {
		return MAPMap.get(sym).functionDaysDiff;
	}

	@Override
	public void setDaysDiff(String sym, Integer daysDiff) {
		MAPMap.get(sym).functionDaysDiff = daysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return MAPMap.get(sym).doubleBacks;
	}

	@Override
	public void setDoubleBacks(String sym, Integer doubleBacks) {
		MAPMap.get(sym).doubleBacks = doubleBacks;
	}

	public double[] getMAs(String sym) {
		return MAPMap.get(sym).MAs;
	}

	public void setMAs(String sym, double[] MAs) {
		MAPMap.get(sym).MAs = MAs;
	}

	@Override
	public void setDateIndex(String sym, TreeMap<String, Integer> dateIndex) {
		MAPMap.get(sym).dateIndex = dateIndex;

	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return MAPMap.get(sym).dateIndex;
	}

}
