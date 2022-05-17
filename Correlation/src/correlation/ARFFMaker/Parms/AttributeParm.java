package correlation.ARFFMaker.Parms;

import java.util.Set;
import java.util.TreeMap;

public interface AttributeParm {

	public Set<String> keySet();

	public TreeMap<String, Integer> getDateIndex(String sym);

	public Integer getDaysDiff(String sym);

	public Integer getDoubleBacks(String sym);

}
