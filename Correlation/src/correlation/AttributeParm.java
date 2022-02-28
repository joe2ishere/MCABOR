package correlation;

import java.util.Set;
import java.util.TreeMap;

public interface AttributeParm {

	public Set<String> keySet();

	public void addSymbol(String sym);

	public TreeMap<String, Integer> getDateIndex(String sym);

	public void setDateIndex(String sym, TreeMap<String, Integer> dateIndex);

	public Integer getDaysDiff(String sym);

	public void setDaysDiff(String sym, Integer daysDiff);

	public Integer getDoubleBacks(String sym);

	public void setDoubleBacks(String sym, Integer doubleBacks);

}
