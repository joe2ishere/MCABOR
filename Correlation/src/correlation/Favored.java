package correlation;

class Favored {
	String symbol;
	double value;

	public Favored(String sym, double val) {
		symbol = sym;
		value = val;
	}

	public void setSymbolValue(String sym, double val) {
		symbol = sym;
		value = val;
	}

	public void addSymbol(String sym) {
		symbol += " " + sym;
	}

	public double getValue() {
		return value;
	}
}