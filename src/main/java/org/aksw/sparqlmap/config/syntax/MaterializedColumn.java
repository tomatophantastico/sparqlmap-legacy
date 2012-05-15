package org.aksw.sparqlmap.config.syntax;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;
@Deprecated
public class MaterializedColumn extends ColumDefinition {
	
	
	private List<Expression> colExpresssions;
	
	
	
	
	public MaterializedColumn(List<Expression> colExpressions) {
		super();
		this.colExpresssions = colExpressions;
	}
	
	
	public Expression getColumnExpression() {
		return FilterUtil.concat(colExpresssions.toArray(new Expression[0]));
	}
	
	
	public List<Expression> getColumnExpressions() {
		return colExpresssions;
	}
	
	
	
	
	
	
	public boolean isResource(){
		throw new ImplementationException("Cannot be asked on materialized cols");
	}
	
	

}
