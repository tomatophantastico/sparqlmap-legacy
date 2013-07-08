package org.aksw.sparqlmap.core.config.syntax.r2rml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionWithString;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.core.mapper.compatibility.CompatibilityChecker;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.FilterUtil;
import org.aksw.sparqlmap.core.mapper.translate.ImplementationException;

public class TermMap{
	
	DataTypeHelper dth;
	
	// column alias from the 
	private Map<String,FromItem>  alias2logicalTable;
	
	//  for each fromItem 
	protected Set<EqualsTo> joinConditions = new HashSet<EqualsTo>();
	
	protected TripleMap trm;
		

	private CompatibilityChecker cchecker;

	/**
	 * the expressions that create a term.
	 */
	private List<Expression> expressions;
	
	
	private LinkedHashMap<String,FromItem> alias2fromItem = new LinkedHashMap<String, FromItem>();

	

	public TermMap(DataTypeHelper dataTypeHelper, List<Expression> expressions, List<FromItem> fromItems, Set<EqualsTo> joinConditions, TripleMap trm) {
			this.dth= dataTypeHelper;
			this.expressions = expressions;

			this.trm = trm;		
			for(FromItem fi: fromItems){
				alias2fromItem.put(fi.getAlias(), fi);
			}
			if(joinConditions!=null){
				this.joinConditions = joinConditions;
			}
			//expressions = new ArrayList<Expression>();
		
		}
	
	
	public TermMap(DataTypeHelper dataTypeHelper, List<Expression> expressions) {
		this.dth= dataTypeHelper;
		this.expressions = expressions;
			
	}


	public List<SelectExpressionItem> getSelectExpressionItems( String colalias){
		List<Expression> exprs = new ArrayList<Expression>(this.getExpressions()); //clone it, so we can work with it
		DataTypeHelper dataTypeHelper = getDataTypeHelper();
		List<SelectExpressionItem> seis = new ArrayList<SelectExpressionItem>();
		//read the header
		
		Expression typeExpr =exprs.remove(0);
		SelectExpressionItem typeSei = new SelectExpressionItem();
		typeSei.setExpression(typeExpr);
		typeSei.setAlias(colalias + ColumnHelper.COL_NAME_RDFTYPE);
		
		seis.add(typeSei);
		
			
		
		//read the litypefield
		Expression litTypeExpr = exprs.remove(0);
		SelectExpressionItem litTypeSei  = new SelectExpressionItem();
		litTypeSei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_TYPE);
		litTypeSei.setExpression(litTypeExpr);
		seis.add(litTypeSei);

		//read the lang
		Expression litlangExpr = exprs.remove(0);
		SelectExpressionItem litnangSei  = new SelectExpressionItem();
		litnangSei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_LANG);
		litnangSei.setExpression(litlangExpr);
		seis.add(litnangSei);
		
	
		//add the string col
		SelectExpressionItem stringsei = new SelectExpressionItem();
		stringsei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_STRING);
		stringsei.setExpression(exprs.remove(0));
		seis.add(stringsei);
		
		//add the numeric col
		SelectExpressionItem numSei = new SelectExpressionItem();
		numSei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_NUMERIC);
		numSei.setExpression(exprs.remove(0));
		seis.add(numSei);
		
		//add the data col
		SelectExpressionItem dateSei = new SelectExpressionItem();
		dateSei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_DATE);
		dateSei.setExpression(exprs.remove(0));
		seis.add(dateSei);
		
		
		//add the bool col
		SelectExpressionItem boolsei = new SelectExpressionItem();
		boolsei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_BOOL);
		boolsei.setExpression(exprs.remove(0));
		seis.add(boolsei);
		
		
		//add the binary col
		SelectExpressionItem binsei = new SelectExpressionItem();
		binsei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_BINARY);
		binsei.setExpression(exprs.remove(0));
		seis.add(binsei);
			
		
		
		//add all the resource expressions
		int i = 0;
		for (Expression expr: exprs) {
			SelectExpressionItem resSei = new SelectExpressionItem();
			resSei.setAlias(colalias + ColumnHelper.COL_NAME_RESOURCE_COL_SEGMENT + i++);
			resSei.setExpression(expr);
			seis.add(resSei);
		}
		return seis;
	}
	
	
	
	public List<FromItem> getFromItems(){
		return new ArrayList<FromItem>(this.alias2fromItem.values());
	}
	
	/**
	 * the join conditions that need to be added to the query for multiple from items.
	 * @return
	 */
	public Set<EqualsTo> getFromJoins(){
		return joinConditions;
		
	}
	
	

	
	public List<Expression> getExpressions() {
		return expressions;
	}
	

	
	
	protected DataTypeHelper getDataTypeHelper(){
		return dth;
	}
	
	public String toString(){
		StringBuffer out = new StringBuffer();
		for(Expression exp: expressions ){
			out.append(exp.toString());
			out.append("|");
		}
		
		return out.toString();
	}
	
	
	public Map<String, FromItem> getAlias2logicalTable() {
		return alias2logicalTable;
	}
	
	protected void setAlias2logicalTable(Map<String, FromItem> alias2logicalTable) {
		this.alias2logicalTable = alias2logicalTable;
	}
	
	
	/**
	 * this method creates a new List and rewrites the aliases of column tables
	 * @return
	 */
	protected List<Expression> cloneColumnExpressions(List<Expression> expressions, String suffix){
		List<Expression> copied = new ArrayList<Expression>();
		
		for (Expression expression : expressions) {
			copied.add(cloneExpression( expression,suffix));
		}
		return copied;
	}


//	private Expression cloneExpr(String suffix, Expression expression) {
//		if(dth.uncast(expression) instanceof Column){
//			Column origCol = (Column) dth.uncast(expression);
//			Column copyCol = cloneExpression(origCol, suffix);
//			return (dth.cast(copyCol,FilterUtil.getCastType(expression)));
//		}else if(dth.uncast(expression) instanceof Function ){
//			Function func = (Function) dth.uncast(expression);
//			List<Expression> copiedConcat  = new ArrayList<Expression>(); 
//			if(func.getName().equals(FilterUtil.CONCAT)){
//				for(Object obj: func.getParameters().getExpressions()){
//					Expression concExpr = (Expression) obj;
//					copiedConcat.add(cloneExpr(suffix, concExpr));
//				}
//			}
//			Function newFunc = new Function();
//			newFunc.setName(func.getName());
//			newFunc.setParameters(new ExpressionList(copiedConcat));
//			return dth.cast(newFunc,FilterUtil.getCastType(expression));
//			
//
//		} else{
//			return (expression);
//		}
//	}
	
	protected Expression cloneExpression(Expression origExpr,String suffix){
		if(origExpr instanceof net.sf.jsqlparser.schema.Column){
			Column origColumn = (Column) origExpr;
			Column copyCol = new Column();
			copyCol.setColumnName(origColumn.getColumnName());
			Table copyTable = new Table(origColumn.getTable().getSchemaName(), origColumn.getTable().getName());
			copyTable.setAlias(origColumn.getTable().getAlias() + suffix);
			copyCol.setTable(copyTable);
			return copyCol;
		}else if(origExpr instanceof CastExpression){
			CastExpression cast = (CastExpression) origExpr;
			
			CastExpression clone = new CastExpression(cloneExpression(cast.getCastedExpression(),suffix), cast.getTypeName()); 
			
			return clone; 
		}else if(origExpr instanceof Function){
			Function origFunction = (Function) origExpr;
			Function clonedFunction = new Function();
			clonedFunction.setAllColumns(origFunction.isAllColumns());
			clonedFunction.setEscaped(origFunction.isEscaped());
			clonedFunction.setName(origFunction.getName());
			List<Expression> cloneExprList = new ArrayList<Expression>();
			if(origFunction.getParameters()!=null){
			for(Object expObj : origFunction.getParameters().getExpressions()){
				cloneExprList.add(cloneExpression((Expression) expObj, suffix));
			}
			}
			clonedFunction.setParameters(new ExpressionList(cloneExprList));
				
			return clonedFunction;
		}else if(origExpr instanceof CastExpression){
			CastExpression origCastExpression = (CastExpression) origExpr;
			CastExpression clonedCastExpression = new CastExpression(cloneExpression(origCastExpression.getCastedExpression(),suffix), origCastExpression.getTypeName());
			return clonedCastExpression;
			
		}else if (origExpr instanceof ExpressionWithString){
			ExpressionWithString orig = (ExpressionWithString) origExpr;
			ExpressionWithString clone = new ExpressionWithString(cloneExpression(orig.getExpression(),suffix),orig.getString());
			return clone;
		}else {
			return origExpr;
		}
	}
	
	
	
	
	
	public TermMap clone(String suffix) {
		
		List<Expression> clonedExpressions = cloneColumnExpressions(this.expressions, suffix);
		
		//rename the from items Accordingly
		
		Collection<FromItem> origFromItems = this.alias2fromItem.values();
		
		final List<FromItem> clonedFromItems = cloneFromItems(suffix,
				origFromItems);
		
		
		Collection<EqualsTo> origjoinConditions = this.joinConditions;
		
		Set<EqualsTo> clonedJoinConditions = cloneJoinsCondition(suffix,
				origjoinConditions);
		
		
		TermMap clone = new TermMap(this.dth, clonedExpressions,clonedFromItems,clonedJoinConditions,trm);
		
		return clone;
	}

	private Set<EqualsTo> cloneJoinsCondition(final String suffix,
			Collection<EqualsTo> origjoinConditions) {
		Set<EqualsTo> clonedJoinConditions = new HashSet<EqualsTo>();
		for(Expression origJoinCon : origjoinConditions){
			
			EqualsTo cloneEq = new EqualsTo();
			Expression leftClone = cloneExpression((((EqualsTo)origJoinCon).getLeftExpression()), suffix);
			 
			cloneEq.setLeftExpression(leftClone);
			Expression rightClone = cloneExpression((((EqualsTo)origJoinCon).getRightExpression()), suffix);
			cloneEq.setRightExpression(rightClone);
			clonedJoinConditions.add(cloneEq);
		}
		return clonedJoinConditions;
	}

	private List<FromItem> cloneFromItems(final String suffix,
			Collection<FromItem> origFromItems) {
		final List<FromItem> clonedFromItems = new ArrayList<FromItem>();
		
		for(FromItem fi : origFromItems){
			fi.accept(new FromItemVisitor() {
				
				@Override
				public void visit(SubJoin subjoin) {
					SubJoin fItem = new SubJoin();
					fItem.setAlias(subjoin.getAlias() + suffix);
					fItem.setJoin(subjoin.getJoin());
					fItem.setLeft(subjoin.getLeft());
					clonedFromItems.add(fItem);
				}
				
				@Override
				public void visit(SubSelect subSelect) {
					SubSelect fItem = new SubSelect();
					fItem.setAlias(subSelect.getAlias() + suffix);
					fItem.setSelectBody(subSelect.getSelectBody());
					clonedFromItems.add(fItem);
					
				}
				
				@Override
				public void visit(Table tableName) {
					FromItem fItem = new Table(tableName.getSchemaName(),tableName.getName());
					fItem.setAlias(tableName.getAlias() + suffix);
					clonedFromItems.add(fItem);
					
				}
			});
			
		}
		return clonedFromItems;
	}
	
	public void addFromItem(FromItem from){
		this.alias2fromItem.put(from.getAlias(), from);
	}
	
	
	
	public void toTtl(StringBuffer sb){
		sb.append("<this> <is> <noTTl>");
	}


	public TripleMap getTripleMap() {
		// TODO Auto-generated method stub
		return trm;
	}
	
	
	public CompatibilityChecker getCompChecker() {
		return cchecker;
	}
	

	public void setCompChecker(CompatibilityChecker cchecker) {
		this.cchecker = cchecker;
	}
	
	
	
	
	
	public static TermMap getNullTermMap(DataTypeHelper dth){
		
		
		
		List<Expression> nullExpressions = new ArrayList<Expression>();
		
		nullExpressions.add(new NullValue());
		nullExpressions.add(dth.cast(new LongValue("1"), dth.getNumericCastType()));
		nullExpressions.add(new NullValue());
		nullExpressions.add(new NullValue());
		nullExpressions.add(new NullValue());
		nullExpressions.add(new NullValue());
		nullExpressions.add(new NullValue());
		
		   
		   
		   
		
		TermMap nullTm = new TermMap(null, nullExpressions);
		return nullTm;
		
	}


	public boolean isConstant() {
		for(Expression ex: expressions){
			if(DataTypeHelper.uncast(ex) instanceof Column){
				return false;
			}
		}
		
		
		return true;
	}
	
	
		
}
