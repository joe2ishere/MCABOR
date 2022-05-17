package correlation.ARFFMaker.Parms;

import java.util.Set;
import java.util.TreeMap;

public class MAParms implements AttributeParm {
	public record MASymbolParm(Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] MAs) {

	}

	TreeMap<String, MASymbolParm> MAPMap;

	public MAParms() {
		MAPMap = new TreeMap<String, MASymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return MAPMap.keySet();
	}

	public void addSymbol(String sym, Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			double[] MAs) {
		MAPMap.put(sym, new MASymbolParm(functionDaysDiff, doubleBacks, dateIndex, MAs));

	}

	@Override
	public Integer getDaysDiff(String sym) {
		return MAPMap.get(sym).functionDaysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return MAPMap.get(sym).doubleBacks;
	}

	public double[] getMAs(String sym) {
		return MAPMap.get(sym).MAs;
	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return MAPMap.get(sym).dateIndex;
	}

}
