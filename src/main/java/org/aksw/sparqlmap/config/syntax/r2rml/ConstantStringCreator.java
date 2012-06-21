package org.aksw.sparqlmap.config.syntax.r2rml;

import static org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil.cast;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;

public class ConstantStringCreator extends ConstantTermCreator{
	
	
	private String string;
	private List<Expression> expressions;

	public ConstantStringCreator(DataTypeHelper dataTypeHelper, String string) {
		super(dataTypeHelper);
		
		expressions = new ArrayList<Expression>();
		expressions.add(cast(new LongValue("2"),dataTypeHelper.getNumericCastType())); //type
		expressions.add(cast(new LongValue("0"),dataTypeHelper.getNumericCastType())); //length
		expressions.add(cast(new LongValue(Integer.toString(Types.VARCHAR)),dataTypeHelper.getNumericCastType())); //littype
		expressions.add(cast(new StringValue("\"" +string +"\""),dataTypeHelper.getStringCastType()));
	}
	
	
	@Override
	public List<Expression> getExpressions() {
		return expressions;
	}

	@Override
	public TermCreator clone(String suffix) {
		ConstantStringCreator clone = new ConstantStringCreator(dataTypeHelper, this.string);
		return clone;
	}

	
	


}
