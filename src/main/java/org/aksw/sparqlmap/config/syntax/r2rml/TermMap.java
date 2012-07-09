package org.aksw.sparqlmap.config.syntax.r2rml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.columnanalyze.CompatibilityChecker;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

public class TermMap{
	
	DataTypeHelper dataTypeHelper;
	
	// column alias from the 
	private Map<String,FromItem>  alias2logicalTable;
	
	//  for each fromItem 
	protected List<EqualsTo> joinConditions;
	
	protected TripleMap trm;
		
	private TermMap original;
	
	private CompatibilityChecker cchecker;
	
	
	

		
	/**
	 * the expressions that create a term.
	 */
	private List<Expression> expressions;
	
	
	private LinkedHashMap<String,FromItem> alias2fromItem = new LinkedHashMap<String, FromItem>();

	

	public TermMap(DataTypeHelper dataTypeHelper, List<Expression> expressions, List<FromItem> fromItems, List<EqualsTo> joinConditions, TripleMap trm) {
			this.dataTypeHelper= dataTypeHelper;
			this.trm = trm;		
			this.expressions = expressions;
			for(FromItem fi: fromItems){
				alias2fromItem.put(fi.getAlias(), fi);
			}
			if(joinConditions!=null){
			this.joinConditions = joinConditions;
			}else{
				this.joinConditions = new ArrayList<EqualsTo>();
			}
			//expressions = new ArrayList<Expression>();
		
		}
	
	
	public TermMap(DataTypeHelper dataTypeHelper, List<Expression> expressions) {
		this.dataTypeHelper= dataTypeHelper;
		this.expressions = expressions;
			
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
		typeSei.setAlias(colalias + ColumnHelper.COL_NAME_RDFTYPE);
		
		seis.add(typeSei);
		
		//read the lengthfield
		Expression resLength=exprs.remove(0);
		Long length = ((LongValue) FilterUtil.uncast(resLength)).getValue();
		SelectExpressionItem resLengthSei = new SelectExpressionItem();
		resLengthSei.setAlias(colalias + ColumnHelper.COL_NAME_RES_LENGTH);
		resLengthSei.setExpression(resLength);
		seis.add(resLengthSei);
		
		
		//read the litypefield
		Expression sqlTypeExpr = exprs.remove(0);
		SelectExpressionItem sqlTypeSei = new SelectExpressionItem();
		sqlTypeSei.setAlias(colalias + ColumnHelper.COL_NAME_SQL_TYPE);
		sqlTypeSei.setExpression(sqlTypeExpr);
		seis.add(sqlTypeSei);
		
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
				
		
		
		
		
		//add all the resource expressions
		for (int i = 0; i < length; i++) {
			SelectExpressionItem resSei = new SelectExpressionItem();
			resSei.setAlias(colalias + ColumnHelper.COL_NAME_RESOURCE_COL_SEGMENT + i);
			resSei.setExpression(exprs.remove(0));
			seis.add(resSei);
		}
			
			
		//Literal, the rest must be literal expressions, decide upon the cast type
		for (Expression expr : exprs) {
			String castType = FilterUtil.getCastType(expr);
			if(castType.equals(dataTypeHelper.getStringCastType())){
				SelectExpressionItem stringsei = new SelectExpressionItem();
				stringsei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_STRING);
				stringsei.setExpression(expr);
				seis.add(stringsei);
				
			}else if( castType.equals(dataTypeHelper.getNumericCastType())){
				SelectExpressionItem numSei = new SelectExpressionItem();
				numSei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_NUMERIC);
				numSei.setExpression(expr);
				seis.add(numSei);
			}else if(castType.equals(dataTypeHelper.getDateCastType())){
				SelectExpressionItem dateSei = new SelectExpressionItem();
				dateSei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_DATE);
				dateSei.setExpression(expr);
				seis.add(dateSei);
			}else{
				throw new ImplementationException("Cast type not supported: " + castType);
			}			
		}	
		return seis;
	}
	
	
	public List<FromItem> getFromIte(){
		List<FromItem> fis = new ArrayList<FromItem>();
		for (Expression expr: getExpressions()) {
			expr = FilterUtil.uncast(expr);
			if(expr instanceof Column){
				fis.add(((Column) expr).getTable());
			}
		}
		return fis;
	}
	
	public List<FromItem> getFromItems(){
		return new ArrayList(this.alias2fromItem.values());
	}
	
	
	public Set<EqualsTo> getFromJoins(){
		if(joinConditions!=null){
			return new HashSet<EqualsTo>(joinConditions);
		}else{
			return new HashSet<EqualsTo>();
		}
		
	}
	
	

	
	public List<Expression> getExpressions() {
		return expressions;
	}
	
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
		
			if(((Column)FilterUtil.uncast(expr)).getColumnName().endsWith(ColumnHelper.COL_NAME_LITERAL_STRING)){
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
			if(((Column)FilterUtil.uncast(expr)).getColumnName().endsWith(ColumnHelper.COL_NAME_LITERAL_NUMERIC)){
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
			if(((Column)FilterUtil.uncast(expr)).getColumnName().endsWith(ColumnHelper.COL_NAME_LITERAL_DATE)){
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
				if(exp instanceof Column && ((Column)exp).getColumnName().contains(ColumnHelper.COL_NAME_RESOURCE_COL_SEGMENT)){
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
	
	
	
	public TermMap clone(final String suffix) {
		
		List<Expression> clonedExpressions = cloneColumnExpressions(this.expressions, suffix);
		
		//rename the from items Accordingly
		
		Collection<FromItem> origFromItems = this.alias2fromItem.values();
		
		final List<FromItem> clonedFromItems = cloneFromItems(suffix,
				origFromItems);
		
		
		Collection<EqualsTo> origjoinConditions = this.joinConditions;
		
		List<EqualsTo> clonedJoinConditions = cloneJoinsCondition(suffix,
				origjoinConditions);
		
		
		TermMap clone = new TermMap(this.dataTypeHelper, clonedExpressions,clonedFromItems,clonedJoinConditions,trm);
		clone.original = this;
		
		return clone;
	}

	private List<EqualsTo> cloneJoinsCondition(final String suffix,
			Collection<EqualsTo> origjoinConditions) {
		List<EqualsTo> clonedJoinConditions = new ArrayList();
		for(Expression origJoinCon : origjoinConditions){
			
			EqualsTo cloneEq = new EqualsTo();
			Expression leftClone = cloneColumn(((Column)((EqualsTo)origJoinCon).getLeftExpression()), suffix);
			cloneEq.setLeftExpression(leftClone);
			Expression rightClone = cloneColumn(((Column)((EqualsTo)origJoinCon).getRightExpression()), suffix);
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
	
	
		
}
