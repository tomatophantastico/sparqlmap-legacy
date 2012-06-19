package org.aksw.sparqlmap.config.syntax;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ColumnHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

public abstract  class TermCreator{
	
	DataTypeHelper dataTypeHelper;
	
	// column alias from the 
	private Map<String,FromItem>  alias2logicalTable;
	
	//  for each fromItem 
	protected List<EqualsTo> joinConditions;
	
	
	
	
	public TermCreator(DataTypeHelper dataTypeHelper) {
		super();
		this.dataTypeHelper = dataTypeHelper;
	}


	public List<SelectExpressionItem> getSelectExpressionItems( String colalias){
		List<Expression> exprs = new ArrayList<Expression>(this.getExpressions()); //clone it, so we can work with it
		DataTypeHelper dataTypeHelper = getDataTypeHelper();
		List<SelectExpressionItem> seis = new ArrayList<SelectExpressionItem>();
		//read the header
		
		Expression typeExpr =exprs.remove(0);
		Long type = ((LongValue) FilterUtil.uncast(typeExpr)).getValue();
		SelectExpressionItem typeSei = new SelectExpressionItem();
		typeSei.setExpression(typeExpr);
		typeSei.setAlias(colalias + ColumnHelper.TYPE_COL);
		
		seis.add(typeSei);
		
		//read the lengthfield
		Expression resLength=exprs.remove(0);
		Long length = ((LongValue) FilterUtil.uncast(resLength)).getValue();
		SelectExpressionItem resLengthSei = new SelectExpressionItem();
		resLengthSei.setAlias(colalias + ColumnHelper.RES_LENGTH_COL);
		resLengthSei.setExpression(resLength);
		seis.add(resLengthSei);
		
		
		//read the litypefield
		Expression sqlTypeExpr = exprs.remove(0);
		SelectExpressionItem sqlTypeSei = new SelectExpressionItem();
		sqlTypeSei.setAlias(colalias + ColumnHelper.SQL_TYPE_COL);
		sqlTypeSei.setExpression(sqlTypeExpr);
		seis.add(sqlTypeSei);
		
		//read the litypefield
				Expression litTypeExpr = exprs.remove(0);
				SelectExpressionItem litTypeSei  = new SelectExpressionItem();
				litTypeSei.setAlias(colalias + ColumnHelper.LIT_TYPE_COL);
				litTypeSei.setExpression(litTypeExpr);
				seis.add(litTypeSei);
		
				//read the lang
				Expression litlangExpr = exprs.remove(0);
				SelectExpressionItem litnangSei  = new SelectExpressionItem();
				litnangSei.setAlias(colalias + ColumnHelper.LIT_LANG_COL);
				litnangSei.setExpression(litlangExpr);
				seis.add(litnangSei);
				
		
		
		
		
		//add all the resource expressions
		for (int i = 0; i < length; i++) {
			SelectExpressionItem resSei = new SelectExpressionItem();
			resSei.setAlias(colalias + ColumnHelper.RESOURCE_COL_SEGMENT + i);
			resSei.setExpression(exprs.remove(0));
			seis.add(resSei);
		}
			
			
		//Literal, the rest must be literal expressions, decide upon the cast type
		for (Expression expr : exprs) {
			String castType = FilterUtil.getCastType(expr);
			if(castType.equals(dataTypeHelper.getStringCastType())){
				SelectExpressionItem stringsei = new SelectExpressionItem();
				stringsei.setAlias(colalias + ColumnHelper.LITERAL_COL_STRING);
				stringsei.setExpression(expr);
				seis.add(stringsei);
				
			}else if( castType.equals(dataTypeHelper.getNumericCastType())){
				SelectExpressionItem numSei = new SelectExpressionItem();
				numSei.setAlias(colalias + ColumnHelper.LITERAL_COL_NUM);
				numSei.setExpression(expr);
				seis.add(numSei);
			}else if(castType.equals(dataTypeHelper.getDateCastType())){
				SelectExpressionItem dateSei = new SelectExpressionItem();
				dateSei.setAlias(colalias + ColumnHelper.LITERAL_COL_DATE);
				dateSei.setExpression(expr);
				seis.add(dateSei);
			}else{
				throw new ImplementationException("Cast type not supported: " + castType);
			}			
		}	
		return seis;
	}
	
	
	public List<FromItem> getFromItems(){
		List<FromItem> fis = new ArrayList<FromItem>();
		for (Expression expr: getExpressions()) {
			expr = FilterUtil.uncast(expr);
			if(expr instanceof Column){
				fis.add(((Column) expr).getTable());
			}
		}
		return fis;
	}
	
	public Set<EqualsTo> getFromJoins(){
		if(joinConditions!=null){
			return new HashSet<EqualsTo>(joinConditions);
		}else{
			return new HashSet<EqualsTo>();
		}
		
	}
	
	

	
	public abstract List<Expression> getExpressions();
	
	public Expression getExpression(){		
		
		
		if(getResourceExpression()!=null && getLiteralExpression()!=null){
			List<Expression> exprs = new ArrayList<Expression>();
			exprs.add(this.getResourceExpression());
			exprs.add(this.getLiteralExpression());
			FilterUtil.coalesce(exprs.toArray(new Expression[0]));
		}
		if(getLiteralExpression()!=null){
			return getLiteralExpression();
		}
		if(getResourceExpression()!=null){
			return getResourceExpression();
		}
		
		//should never get here
		throw new ImplementationException("Sth. went horribly wrong");
		
		
		 
	}
	
	
	protected DataTypeHelper getDataTypeHelper(){
		return dataTypeHelper;
	}
	
	public String toString(){
		return getExpression().toString();
	}
	
	
	public Map<String, FromItem> getAlias2logicalTable() {
		return alias2logicalTable;
	}
	
	protected void setAlias2logicalTable(Map<String, FromItem> alias2logicalTable) {
		this.alias2logicalTable = alias2logicalTable;
	}
	
	
	public abstract TermCreator clone(String suffix);
	/**
	 * this method creates a new List and rewrites the aliases of column tables
	 * @return
	 */
	protected List<Expression> cloneColumnExpressions(List<Expression> expressions, String suffix){
		List<Expression> copied = new ArrayList<Expression>();
		
		for (Expression expression : expressions) {
			if(FilterUtil.uncast(expression) instanceof Column){
				Column origCol = (Column) FilterUtil.uncast(expression);
				Column copyCol = cloneColumn(origCol, suffix);
				copied.add(FilterUtil.cast(copyCol,FilterUtil.getCastType(expression)));
			}else{
				copied.add(expression);
			}
		}
		return copied;
	}
	
	protected Column cloneColumn(Column origCol,String suffix){
		Column copyCol = new Column();
		copyCol.setColumnName(origCol.getColumnName());
		Table copyTable = new Table(origCol.getTable().getSchemaName(), origCol.getTable().getName());
		copyTable.setAlias(origCol.getTable().getAlias() + suffix);
		copyCol.setTable(copyTable);
		return copyCol;
	}
	
	
	/**
	 * returns the expressions, representing the literal of this term. If it has any. Uncasting might be required.
	 * @return
	 */
	public Expression getLiteralStringExpression(){
		
		for(Expression expr: getLiteralExpressions()){
			String type = FilterUtil.getCastType(expr);
			if(type != null && type.equals(dataTypeHelper.getStringCastType())){
				return expr;
			}
		
			if(((Column)FilterUtil.uncast(expr)).getColumnName().endsWith(ColumnHelper.LITERAL_COL_STRING)){
				return expr;
			}
		}
		return null;
	}
	
	public Expression getLiteralNumericExpression(){
		for(Expression expr: getLiteralExpressions()){
			String type = FilterUtil.getCastType(expr);
			if(type != null && type.equals(dataTypeHelper.getNumericCastType())){
				return expr;
			}
			if(((Column)FilterUtil.uncast(expr)).getColumnName().endsWith(ColumnHelper.LITERAL_COL_NUM)){
				return expr;
			}
		}
		return null;
	}
	
	public Expression getLiteralDateExpression(){
		for(Expression expr: getLiteralExpressions()){
			String type = FilterUtil.getCastType(expr);
			if(type != null && type.equals(dataTypeHelper.getDateCastType())){
				return expr;
			}
			if(((Column)FilterUtil.uncast(expr)).getColumnName().endsWith(ColumnHelper.LITERAL_COL_DATE)){
				return expr;
			}
		}
		return null;
	}
	
	public List<Expression> getResourceExpressions(){
		List<Expression> expressions = new ArrayList<Expression>();
		int length = getLength();
		for(int i = 0; i < length; i++){
			expressions.add(getExpressions().get(i+5));
		}
		return expressions;
	}
	
	
	public Expression getResourceExpression(){
		if(getResourceExpressions()!=null && getResourceExpressions().size()>0){
		Function concat = FilterUtil.concat(getResourceExpressions().toArray(new Expression[0]));
		return concat;
		}
			return null;
		
		
	}
	
	public Expression getLiteralExpression(){
		List<Expression> lits = new ArrayList<Expression>();
		if(getLiteralDateExpression()!=null){
		lits.add(getLiteralDateExpression());
		}
		if(getLiteralNumericExpression()!=null){
			lits.add(getLiteralNumericExpression());
		}
		if (getLiteralStringExpression()!=null){
			lits.add(getLiteralStringExpression());
		}
		
		if(lits.size()>1){
			return FilterUtil.coalesce(lits.toArray(new Expression[0]));
		}else if(lits.size() == 1){
			return lits.get(0);
		}
		return null;
		
	}
	
	
	
	public List<Expression> getLiteralExpressions(){
		//jump to the literals
				List<Expression> expressions = new ArrayList<Expression>(getExpressions());
				int length = getLength();
				for(int i = 0; i< length+5; i++){
					expressions.remove(0);
				}
				return expressions;		
	}
	
	
	
	public Integer getLength(){
		if(FilterUtil.uncast(getExpressions().get(1)) instanceof LongValue){
			return  (int) ((LongValue)FilterUtil.uncast(getExpressions().get(1))).getValue();			
		}else{
			int i = 0;
			for (Expression exp : getExpressions()) {
				if(exp instanceof Column && ((Column)exp).getColumnName().contains(ColumnHelper.RESOURCE_COL_SEGMENT)){
					i++;
				}
			}
			return i;
		}
		
		
		
	}
	
	public Integer getType(){
		if(FilterUtil.uncast(getExpressions().get(0)) instanceof LongValue){
			return  (int) ((LongValue)FilterUtil.uncast(getExpressions().get(0))).getValue();
		}else{
			return null;
		}
		
		
	}
	
	public Integer getSqlType(){
		if(FilterUtil.uncast(getExpressions().get(2)) instanceof LongValue){
			return  (int) ((LongValue)FilterUtil.uncast(getExpressions().get(2))).getValue();
		}else{
			return null;
		}
		
	}
	public Expression getLanguage(){
		
		return FilterUtil.uncast(getExpressions().get(4));

	}
	public String  getDataType(){
		if(FilterUtil.uncast(getExpressions().get(5)) instanceof StringValue){
			return  (String) ((StringValue)FilterUtil.uncast(getExpressions().get(3))).getValue();
		}else{
			return null;
		}
		
	}
	
	

	
	
	
		
}
