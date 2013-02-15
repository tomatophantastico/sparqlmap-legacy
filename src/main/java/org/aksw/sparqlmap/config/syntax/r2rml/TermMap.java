package org.aksw.sparqlmap.config.syntax.r2rml;

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

import org.aksw.sparqlmap.mapper.compatibility.CompatibilityChecker;
import org.aksw.sparqlmap.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.mapper.translate.FilterUtil;
import org.aksw.sparqlmap.mapper.translate.ImplementationException;

public class TermMap{
	
	DataTypeHelper dth;
	
	// column alias from the 
	private Map<String,FromItem>  alias2logicalTable;
	
	//  for each fromItem 
	protected Set<EqualsTo> joinConditions;
	
	protected TripleMap trm;
		
	private TermMap original;
	
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
			}else{
				this.joinConditions = new HashSet<EqualsTo>();
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
		Long type = ((LongValue) DataTypeHelper.uncast(typeExpr)).getValue();
		SelectExpressionItem typeSei = new SelectExpressionItem();
		typeSei.setExpression(typeExpr);
		typeSei.setAlias(colalias + ColumnHelper.COL_NAME_RDFTYPE);
		
		seis.add(typeSei);
		
		//read the lengthfield
		Expression resLength=exprs.remove(0);
		Long length = ((LongValue) DataTypeHelper.uncast(resLength)).getValue();
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
		
		//read the graph
		Expression graphExpr = exprs.remove(0);
		SelectExpressionItem graphSei  = new SelectExpressionItem();
		graphSei.setAlias(colalias + ColumnHelper.COL_NAME_GRAPH);
		graphSei.setExpression(graphExpr);
		seis.add(graphSei);		
		
		
		
		
		//add all the resource expressions
		for (int i = 0; i < length; i++) {
			SelectExpressionItem resSei = new SelectExpressionItem();
			resSei.setAlias(colalias + ColumnHelper.COL_NAME_RESOURCE_COL_SEGMENT + i);
			resSei.setExpression(exprs.remove(0));
			seis.add(resSei);
		}
			
			
		//Literal, the rest must be literal expressions, decide upon the cast type
		for (Expression expr : exprs) {
			String castType = DataTypeHelper.getCastType(expr);
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
			}else if(castType.equals(dataTypeHelper.getBooleanCastType())){
				SelectExpressionItem boolsei = new SelectExpressionItem();
				boolsei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_BOOL);
				boolsei.setExpression(expr);
				seis.add(boolsei);
			}else if(castType.equals(dataTypeHelper.getBinaryDataType())){
				SelectExpressionItem binsei = new SelectExpressionItem();
				binsei.setAlias(colalias + ColumnHelper.COL_NAME_LITERAL_BINARY);
				binsei.setExpression(expr);
				seis.add(binsei);
			}else
			{
				throw new ImplementationException("Cast type not supported: " + castType);
			}			
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
		return dth;
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
	
	
	/**
	 * returns the expressions, representing the literal of this term. If it has any. Uncasting might be required.
	 * @return
	 */
	public Expression getLiteralStringExpression(){
		
		for(Expression expr: getLiteralExpressions()){
			String type = DataTypeHelper.getCastType(expr);
			if(type != null && type.equals(dth.getStringCastType())){
				return expr;
			}
		
			if(DataTypeHelper.uncast(expr)instanceof Column &&((Column)DataTypeHelper.uncast(expr)).getColumnName().endsWith(ColumnHelper.COL_NAME_LITERAL_STRING)){
				return expr;
			}
		}
		return null;
	}
	
	private Expression getLiteralBoolExpression() {
		for(Expression expr: getLiteralExpressions()){
			String type = DataTypeHelper.getCastType(expr);
			if(type != null && type.equals(dth.getBooleanCastType())){
				return expr;
			}
		
			if(DataTypeHelper.uncast(expr)instanceof Column &&((Column)DataTypeHelper.uncast(expr)).getColumnName().endsWith(ColumnHelper.COL_NAME_LITERAL_BOOL)){
				return expr;
			}
		}
		return null;
	}
	
	public Expression getLiteralNumericExpression(){
		for(Expression expr: getLiteralExpressions()){
			String type = DataTypeHelper.getCastType(expr);
			if(type != null && type.equals(dth.getNumericCastType())){
				return expr;
			}
			if(DataTypeHelper.uncast(expr)instanceof Column &&((Column)DataTypeHelper.uncast(expr)).getColumnName().endsWith(ColumnHelper.COL_NAME_LITERAL_NUMERIC)){
				return expr;
			}
		}
		return null;
	}
	
	public Expression getLiteralBinaryExpression(){
		for(Expression expr: getLiteralExpressions()){
			String type = DataTypeHelper.getCastType(expr);
			if(type != null && type.equals(dth.getBinaryDataType())){
				return expr;
			}
			if(DataTypeHelper.uncast(expr)instanceof Column &&((Column)DataTypeHelper.uncast(expr)).getColumnName().endsWith(ColumnHelper.COL_NAME_LITERAL_BINARY)){
				return expr;
			}
		}
		return null;
	}
	
	public Expression getLiteralDateExpression(){
		for(Expression expr: getLiteralExpressions()){
			String type = DataTypeHelper.getCastType(expr);
			if(type != null && type.equals(dth.getDateCastType())){
				return expr;
			}
			if(DataTypeHelper.uncast(expr)instanceof Column &&((Column)DataTypeHelper.uncast(expr)).getColumnName().endsWith(ColumnHelper.COL_NAME_LITERAL_DATE)){
				return expr;
			}
		}
		return null;
	}
	
	public List<Expression> getResourceExpressions(){
		List<Expression> expressions = new ArrayList<Expression>();
		int length = getLength();
		for(int i = 0; i < length; i++){
			expressions.add(getExpressions().get(i+6));
		}
		return expressions;
	}
	
	
	public Expression getResourceExpression(){
		if(getResourceExpressions()!=null && getResourceExpressions().size()>0){
		Expression concat = FilterUtil.concat(getResourceExpressions().toArray(new Expression[0]));
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
		if (getLiteralBoolExpression()!=null){
			lits.add(getLiteralBoolExpression());
		}
		if (getLiteralBinaryExpression()!=null){
			lits.add(getLiteralBinaryExpression());
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
				for(int i = 0; i< length+6; i++){
					expressions.remove(0);
				}
				return expressions;		
	}
	
	
	
	public Integer getLength(){
		if(DataTypeHelper.uncast(getExpressions().get(1)) instanceof LongValue){
			return  (int) ((LongValue)DataTypeHelper.uncast(getExpressions().get(1))).getValue();			
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
		if(DataTypeHelper.uncast(getExpressions().get(0)) instanceof LongValue){
			return  (int) ((LongValue)DataTypeHelper.uncast(getExpressions().get(0))).getValue();
		}else{
			return null;
		}
		
		
	}
	
	public Integer getSqlType(){
		if(DataTypeHelper.uncast(getExpressions().get(2)) instanceof LongValue){
			return  (int) ((LongValue)DataTypeHelper.uncast(getExpressions().get(2))).getValue();
		}else{
			return null;
		}
		
	}
	public Expression getLanguage(){
		
		return DataTypeHelper.uncast(getExpressions().get(4));

	}
	public String  getDataType(){
		if(DataTypeHelper.uncast(getExpressions().get(5)) instanceof net.sf.jsqlparser.expression.StringValue){
			return  (String) ((StringValue)DataTypeHelper.uncast(getExpressions().get(3))).getValue();
		}else{
			return null;
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
		clone.original = this;
		
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
	
	
	
	
	
	public static TermMap getNullTermMap(){
		
		List<Expression> nullExpressions = new ArrayList<Expression>();
		
		 for(int i = 0; i<6;i++){
			 nullExpressions.add(new NullValue());
		 }
		   
		   
		   
		
		TermMap nullTm = new TermMap(null, nullExpressions);
		return nullTm;
		
	}
	
	
		
}
