package correlation;

import java.util.Set;
import java.util.TreeMap;

public class MACDParms implements AttributeParm {
	public class MACDSymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		TreeMap<String, Integer> dateIndex;

		double[] macd;
		double[] signal;
	}

	TreeMap<String, MACDSymbolParm> MACDPMap;

	public MACDParms() {
		MACDPMap = new TreeMap<String, MACDSymbolParm>();
	}

	@Override
	public Set<String> keySet() {
		return MACDPMap.keySet();
	}

	@Override
	public void addSymbol(String sym) {
		MACDPMap.put(sym, new MACDSymbolParm());
	}

	@Override
	public Integer getDaysDiff(String sym) {
		return MACDPMap.get(sym).functionDaysDiff;
	}

	@Override
	public void setDaysDiff(String sym, Integer daysDiff) {
		MACDPMap.get(sym).functionDaysDiff = daysDiff;
	}

	@Override
	public Integer getDoubleBacks(String sym) {
		return MACDPMap.get(sym).doubleBacks;
	}

	@Override
	public void setDoubleBacks(String sym, Integer doubleBacks) {
		MACDPMap.get(sym).doubleBacks = doubleBacks;
	}

	public double[] getMACD(String sym) {
		return MACDPMap.get(sym).macd;
	}

	public void setMACD(String sym, double[] macd) {
		MACDPMap.get(sym).macd = macd;
	}

	public double[] getSignal(String sym) {
		return MACDPMap.get(sym).signal;
	}

	public void setSignal(String sym, double[] signal) {
		MACDPMap.get(sym).signal = signal;
	}

	@Override
	public void setDateIndex(String sym, TreeMap<String, Integer> dateIndex) {
		MACDPMap.get(sym).dateIndex = dateIndex;

	}

	@Override
	public TreeMap<String, Integer> getDateIndex(String sym) {

		return MACDPMap.get(sym).dateIndex;
	}

}
