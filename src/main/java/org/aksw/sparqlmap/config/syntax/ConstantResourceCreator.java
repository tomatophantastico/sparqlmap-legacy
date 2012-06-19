package org.aksw.sparqlmap.config.syntax;

import static org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil.cast;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;

public class ConstantResourceCreator extends ConstantTermCreator{
	String iri;
	List<Expression> expressions;
	public ConstantResourceCreator(DataTypeHelper dataTypeHelper, String resourceIri) {
		super(dataTypeHelper);
		this.iri = resourceIri;
		
		expressions = new ArrayList<Expression>();
		expressions.add(cast(new LongValue("1"),dataTypeHelper.getNumericCastType())); //type
		expressions.add(cast(new LongValue("1"),dataTypeHelper.getNumericCastType())); //length
		expressions.add(cast(new LongValue("-99"),dataTypeHelper.getNumericCastType())); //littype
		expressions.add(cast(new net.sf.jsqlparser.expression.NullValue(), dataTypeHelper.getStringCastType()));
		expressions.add(cast(new NullValue(), dataTypeHelper.getStringCastType()));
		expressions.add(cast(new StringValue("\"" +resourceIri +"\""),dataTypeHelper.getStringCastType()));
	}
	@Override
	public List<Expression> getExpressions() {
		return expressions;
	}
	@Override
	public TermCreator clone(String suffix) {
		
		return new ConstantResourceCreator(getDataTypeHelper(), iri);
	}

}
