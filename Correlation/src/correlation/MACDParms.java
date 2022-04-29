package correlation;

import java.util.Set;
import java.util.TreeMap;

public class MACDParms implements AttributeParm {
	public record MACDSymbolParm(Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,

			double[] macd, double[] signal) {

	}

	TreeMap<String, MACDSymbolParm> MACDPMap;

	public MACDParms() {
		MACDPMap = new TreeMap<String, MACDSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return MACDPMap.keySet();
	}

	public void addSymbol(String sym, Integer functionDaysDiff, Integer doubleBacks, TreeMap<String, Integer> dateIndex,

			double[] macd, double[] signal) {
		MACDPMap.put(sym, new MACDSymbolParm(functionDaysDiff, doubleBacks, dateIndex, macd, signal));
	}

	@Override
	public Integer getDaysDiff(String sym) {
		return MACDPMap.get(sym).functionDaysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return MACDPMap.get(sym).doubleBacks;
	}

	public double[] getMACD(String sym) {
		return MACDPMap.get(sym).macd;
	}

	public double[] getSignal(String sym) {
		return MACDPMap.get(sym).signal;
	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return MACDPMap.get(sym).dateIndex;
	}

}
