package net.sf.jsqlparser.statement.select;

import net.sf.jsqlparser.expression.Expression;

public class OrderByExpressionElement extends OrderByElement {
	
	Expression expr;

	public OrderByExpressionElement(Expression expr) {
		super();
		this.expr = expr;
	}
	
	public Expression getExpression() {
		return expr;
	}
	
	
	
	
	

}
