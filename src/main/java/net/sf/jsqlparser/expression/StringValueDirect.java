package net.sf.jsqlparser.expression;

public class StringValueDirect extends StringValue {

	public StringValueDirect(String unescapedValue) {
		super("\"" + unescapedValue + "\"");
	}

}
