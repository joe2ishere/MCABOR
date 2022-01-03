package correlation;

import java.util.Set;
import java.util.TreeMap;

public class MACDParms implements AttributeParm {
	public class MACDSymbolParm {
		Integer functionDaysDiff;
		Integer doubleBacks;
		String[] dates;
		int lastDateStart;
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
		MACDPMap.get(sym).lastDateStart = 0;
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
	public String[] getAttrDates(String sym) {
		return MACDPMap.get(sym).dates;
	}

	@Override
	public void setAttrDates(String sym, String dates[]) {
		MACDPMap.get(sym).dates = dates;
	}

	@Override
	public int getLastDateStart(String sym) {
		return MACDPMap.get(sym).lastDateStart;
	}

	@Override
	public void setLastDateStart(String sym, int start) {
		MACDPMap.get(sym).lastDateStart = start;
	}

}
