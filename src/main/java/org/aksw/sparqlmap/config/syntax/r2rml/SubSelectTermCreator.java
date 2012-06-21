package org.aksw.sparqlmap.config.syntax.r2rml;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.statement.select.FromItem;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

public class SubSelectTermCreator extends TermCreator{
	
	static SubSelectTermCreator deconcat(Expression concatTermExpr, DataTypeHelper dth){
		
		if(concatTermExpr instanceof Function && ((Function) concatTermExpr).getName().equals("CONCAT")){
			List<Expression> exprs = ((Function) concatTermExpr).getParameters().getExpressions();
			if(FilterUtil.uncast(exprs.get(0)) instanceof LongValue && FilterUtil.uncast(exprs.get(0)) instanceof LongValue &&FilterUtil.uncast(exprs.get(0)) instanceof LongValue && exprs.size()>3){
				return new SubSelectTermCreator(dth, exprs);
			}
		}
		
		return null;
		
		
	}
	
	List<Expression> expressions;

	public SubSelectTermCreator(DataTypeHelper dataTypeHelper, List<Expression> expressions, List<FromItem> fromItems) {
		super(dataTypeHelper);
		this.expressions = expressions;
		
	}
	
	/**
	 * use this for checking in filters, etc.
	 * @param dataTypeHelper
	 * @param expressions
	 */
	public SubSelectTermCreator(DataTypeHelper dataTypeHelper, List<Expression> expressions) {
		super(dataTypeHelper);
		this.expressions = expressions;
		
	}
	
	

	@Override
	public List<Expression> getExpressions() {
		// TODO Auto-generated method stub
		return expressions;
	}

	@Override
	public TermCreator clone(String suffix) {
		throw new ImplementationException("Implement cloning of subselect based term creators.");
	}

	
	
}
