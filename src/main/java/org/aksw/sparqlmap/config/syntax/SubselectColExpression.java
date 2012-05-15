package org.aksw.sparqlmap.config.syntax;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

public class SubselectColExpression implements Expression {

	@Override
	public void accept(ExpressionVisitor expressionVisitor) {
		throw new ImplementationException("This class is not meant to be used in an actual query.");

	}

}
