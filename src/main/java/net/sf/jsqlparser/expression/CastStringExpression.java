package net.sf.jsqlparser.expression;

public class CastStringExpression extends StringValueDirect{

	private String col;
	private String castto;
	private String table;
	private Expression expr;
	
	public CastStringExpression(String table , String col, String castto) {
		super(table + "." + col
				+ " AS " + castto);
		this.col = col;
		this.castto = castto;
		this.table = table;

	}
	
	public CastStringExpression( String castto) {
		super("NULL AS " + castto);
		this.castto = castto;


	}
	
	public CastStringExpression(Expression expr,  String castto) {
		super( expr.toString() + " AS " + castto);
		this.castto = castto;
		this.expr = expr;

	}
	
	
	
	
	public String getValue(Character escape){
		if(table==null&&col==null&&expr==null){
			return "NULL AS " + castto;
		}if(expr!=null){
			return " AS " + castto;
		}	else{
			return escape + table +escape + "." +escape + col +escape 	+ " AS " + castto;
		}
		
		
	}
	
	
	@Override
	public void accept(ExpressionVisitor expressionVisitor) {
			if(expr!=null){
			expr.accept(expressionVisitor);
			}
			expressionVisitor.visit(this);

	}
	
	
	public Expression getExpr() {
		return expr;
	}

	public String getCastto() {
		return castto;
	}
}
