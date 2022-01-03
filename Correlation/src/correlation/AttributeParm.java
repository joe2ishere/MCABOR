package correlation;

import java.util.Set;

public interface AttributeParm {

	public Set<String> keySet();

	public void addSymbol(String sym);

	public String[] getAttrDates(String sym);

	public void setAttrDates(String sym, String dates[]);

	public int getLastDateStart(String sym);

	public void setLastDateStart(String sym, int start);

	public Integer getDaysDiff(String sym);

	public void setDaysDiff(String sym, Integer daysDiff);

	public Integer getDoubleBacks(String sym);

	public void setDoubleBacks(String sym, Integer doubleBacks);
}
