package correlation.ARFFMaker.Parms;

import java.util.Set;
import java.util.TreeMap;

import StochasticMomentum.StochasticMomentum;

public class SMIParms implements AttributeParm {

	public record SMISymbolParm(Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			StochasticMomentum smis) {
	}

	public SMIParms() {
		SMIPMap = new TreeMap<String, SMISymbolParm>();
	}

	TreeMap<String, SMISymbolParm> SMIPMap;

	@Override
	public Set<String> keySet() {
		return SMIPMap.keySet();
	}

	public void addSymbol(String sym, Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,
			StochasticMomentum smis) {
		SMIPMap.put(sym, new SMISymbolParm(functionDaysDiff, doubleBacks, dateIndex, smis));

	}

	public Integer getDaysDiff(String sym) {
		return SMIPMap.get(sym).functionDaysDiff;
	}

	public Integer getDoubleBacks(String sym) {
		return SMIPMap.get(sym).doubleBacks;
	}

	public StochasticMomentum getSMIs(String sym) {
		return SMIPMap.get(sym).smis;
	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {
		return SMIPMap.get(sym).dateIndex;
	}

}
