package org.aksw.sparqlmap.config.syntax;

import static org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil.cast;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;

public abstract class ConstantNumericTermCreator extends ConstantTermCreator{
	Double d;
	List<Expression> expressions;
	public ConstantNumericTermCreator(DataTypeHelper dataTypeHelper, Double d) {
		super(dataTypeHelper);
		 expressions = new ArrayList<Expression>();
		expressions.add(cast(new LongValue("2"),dataTypeHelper.getNumericCastType())); //type
		expressions.add(cast(new LongValue("0"),dataTypeHelper.getNumericCastType())); //length
		expressions.add(cast(new LongValue(Integer.toString(Types.NUMERIC)),dataTypeHelper.getNumericCastType())); //littype
		expressions.add(cast(new DoubleValue("\"" +d+"\""),dataTypeHelper.getNumericCastType()));
	}
	
	@Override
	public List<Expression> getExpressions() {
		return expressions;
	}



}
