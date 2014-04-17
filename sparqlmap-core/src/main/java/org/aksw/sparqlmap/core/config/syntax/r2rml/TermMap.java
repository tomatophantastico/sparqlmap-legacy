package org.aksw.sparqlmap.core.config.syntax.r2rml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionWithString;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.mapper.compatibility.CompatibilityChecker;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.FilterUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.hp.hpl.jena.rdf.model.Resource;

public class TermMap{
	
	org.slf4j.Logger log = LoggerFactory.getLogger(TermMap.class); 
	
	DataTypeHelper dth;

	
	//  for each fromItem 
	protected Set<EqualsTo> joinConditions = new HashSet<EqualsTo>();
	
	protected TripleMap trm;
		
	private CompatibilityChecker cchecker;

	public Expression termType;
	public Expression literalType;
	public Expression literalLang;
	public Expression literalValString;
	public Expression literalValNumeric;
	public Expression literalValDate;
	public Expression literalValBool;
	public Expression literalValBinary;
	
	public List<Expression> resourceColSeg = new ArrayList<Expression>(); 
	
	protected LinkedHashMap<String,FromItem> alias2fromItem = new LinkedHashMap<String, FromItem>();

	

//	public static TermMap createTermMap(DataTypeHelper dataTypeHelper,
//			List<Expression> expressions, List<FromItem> fromItems,
//			Set<EqualsTo> joinConditions, TripleMap trm) {
//		TermMap tm = new TermMap(dataTypeHelper);
//		tm.setExpressions(expressions);
//
//		tm.trm = trm;
//		for (FromItem fi : fromItems) {
//			tm.alias2fromItem.put(fi.getAlias(), fi);
//		}
//		if (joinConditions != null) {
//			tm.joinConditions = joinConditions;
//		}
//		// expressions = new ArrayList<Expression>();
//		return tm;
//	}
	
	private void setExpressions(List<Expression> expressions) {
		List<Expression> exprs = new ArrayList<Expression>(expressions); //clone it, so we can work with it
		
		termType = exprs.remove(0);
		literalType = exprs.remove(0);
		literalLang= exprs.remove(0);
		literalValString= exprs.remove(0);
		literalValNumeric  = exprs.remove(0);
		literalValDate = exprs.remove(0);
		literalValBool = exprs.remove(0);
		literalValBinary = exprs.remove(0);
		resourceColSeg.addAll(exprs);
				
	}


	public static  TermMap createTermMap(DataTypeHelper dataTypeHelper, Collection<Expression> expressions) {
		TermMap tm = new TermMap(dataTypeHelper);
		tm.setExpressions(new ArrayList<Expression>(expressions));
		
		return tm;
			
	}
	
	public TermMap(DataTypeHelper dth){
		this.dth = dth;
		termType = dth.castNull(dth.getNumericCastType());
		literalType = dth.castNull(dth.getStringCastType());
		literalLang= dth.castNull(dth.getStringCastType());
		literalValString= dth.castNull(dth.getStringCastType());
		literalValNumeric  = dth.castNull(dth.getNumericCastType());
		literalValDate = dth.castNull(dth.getDateCastType());
		literalValBool = dth.castNull(dth.getBooleanCastType());
		literalValBinary = dth.castNull(dth.getBinaryDataType());
		
		resourceColSeg = new ArrayList<Expression>(); 
	}
	


	public List<SelectExpressionItem> getSelectExpressionItems( String colalias){
		List<Expression> exprs = new ArrayList<Expression>(this.getExpressions()); //clone it, so we can work with it
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
		return Collections.unmodifiableList(new ArrayList<FromItem>(this.alias2fromItem.values()));
	}
	
	/**
	 * the join conditions that need to be added to the query for multiple from items.
	 * @return
	 */
	public Set<EqualsTo> getFromJoins(){
		return joinConditions;
		
	}
	
	
	protected DataTypeHelper getDataTypeHelper(){
		return dth;
	}
	
	public String toString(){
		StringBuffer out = new StringBuffer();
		out.append("||");
		for(Expression exp: getValueExpressions() ){
			if(! (DataTypeHelper.uncast(exp) instanceof NullValue)){
				out.append(DataTypeHelper.uncast( exp).toString());
				out.append("|");
			}
			
		}
		out.append("|");
		
		return out.toString();
	}
	

	
	
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
		
		TermMap clone = new TermMap(this.dth);
		clone.cchecker = cchecker;
		clone.termType = cloneExpression(termType, suffix);
		clone.literalLang = cloneExpression(literalLang, suffix);
		clone.literalType = cloneExpression(literalType, suffix);
		clone.literalValBinary  = cloneExpression(literalValBinary, suffix);
		clone.literalValBool = cloneExpression(literalValBool, suffix);
		clone.literalValDate = cloneExpression(literalValDate, suffix);
		clone.literalValNumeric = cloneExpression(literalValNumeric, suffix);
		clone.literalValString = cloneExpression(literalValString, suffix);
		
		for(Expression resourceSegment :resourceColSeg){
			clone.resourceColSeg.add(cloneExpression(resourceSegment, suffix));
		}
		
		for(FromItem fi : alias2fromItem.values()){
			FromItem ficlone = cloneFromItem(suffix,fi);
			clone.alias2fromItem.put(ficlone.getAlias(), ficlone);
		}
		
		for(EqualsTo joinCondition : joinConditions){
			clone.joinConditions.add(cloneJoinCondition(suffix, joinCondition));
		}
		
		
		
	
			
		return clone;
	}

	private EqualsTo cloneJoinCondition(final String suffix,
			EqualsTo origjoinCondition) {

		EqualsTo cloneEq = new EqualsTo();
		Expression leftClone = cloneExpression(
				(((EqualsTo) origjoinCondition).getLeftExpression()), suffix);

		cloneEq.setLeftExpression(leftClone);
		Expression rightClone = cloneExpression(
				(((EqualsTo) origjoinCondition).getRightExpression()), suffix);
		cloneEq.setRightExpression(rightClone);

		return cloneEq;
	}

	private FromItem cloneFromItem(final String suffix,
			final FromItem fi) {
		
		final List<FromItem> clonedFromItems = new ArrayList<FromItem>();

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
			
			if(clonedFromItems.size()!=1){
				throw new ImplementationException("Error cloning a term map.");
			}
			
			return clonedFromItems.get(0);

	}
	
	public void addFromItem(FromItem from){
		this.alias2fromItem.put(from.getAlias(), from);
	}
	
	
	
	public TripleMap getTripleMap() {
		return trm;
	}
	
	
	public CompatibilityChecker getCompChecker() {
		return cchecker;
	}
	

	public void setCompChecker(CompatibilityChecker cchecker) {
		this.cchecker = cchecker;
	}
	
	public static TermMap createNullTermMap(DataTypeHelper dth){
		TermMap nullTm = new TermMap(dth);
		
		return nullTm;
		
	}

	public boolean isConstant() {
		for(Expression ex: getExpressions()){
			if(DataTypeHelper.uncast(ex) instanceof Column){
				return false;
			}
		}
		
		
		return true;
	}
	/**
	 * get all expressions that consitute this term map
	 * @return
	 */
	public List<Expression> getExpressions(){
		
		List<Expression> exprs = Lists.newArrayList(
				termType,
				literalType,
				literalLang,
				literalValString,
				literalValNumeric,
				literalValDate,
				literalValBool,
				literalValBinary);
		exprs.addAll(resourceColSeg);
		
		return exprs;
	}
	
	/**
	 * get all expressions that can hold the str() value of an term map
	 * @return
	 */
	public List<Expression> getValueExpressions(){
		
		List<Expression> exprs = Lists.newArrayList(
				literalValString,
				literalValNumeric,
				literalValDate,
				literalValBool,
				literalValBinary);
		exprs.addAll(resourceColSeg);
		
		return exprs;
	}
	
	
	public void setTermTyp(Resource tt){
		if(tt.equals(R2RML.IRI)){
			termType =dth.asNumeric(ColumnHelper.COL_VAL_TYPE_RESOURCE);
		}else if (tt.equals(R2RML.BlankNode)) {
			termType = dth.asNumeric(ColumnHelper.COL_VAL_TYPE_BLANK);
		} else if (tt.equals(R2RML.Literal)) {
			termType = dth.asNumeric(ColumnHelper.COL_VAL_TYPE_LITERAL);
		}
	}
	

	
	
	public Resource getTermTypeAsResource(){
		String tt = ((LongValue)DataTypeHelper.uncast(termType)).getStringValue();
		
		if(tt.equals(ColumnHelper.COL_VAL_TYPE_RESOURCE.toString())){
			return R2RML.IRI;
		}else if (tt.equals(ColumnHelper.COL_VAL_TYPE_BLANK.toString())) {
			return R2RML.BlankNode;
		} else{
			return R2RML.Literal;
		}
	}
	
	public void setLiteralDataType(String ldt){
		if(ldt!=null&&!ldt.isEmpty()){
			this.literalType = dth.cast(new StringValue("'"+ldt+"'"), dth.getStringCastType()); 
		}
		
	}
	
	
	public List<Expression> getResourceColSeg() {
		return resourceColSeg;
	}
	
	public Expression getLiteralValBool() {
		return literalValBool;
	}
	
	
	
	@Override
	public int hashCode() {
		HashCodeBuilder hcb = new HashCodeBuilder();
		for(Expression expr: getExpressions()){
			hcb.append(expr.toString());
		}
		
		return hcb.toHashCode();
	}
	
	
	
	@Override
	public boolean equals(Object obj) {

		   if (obj == null) { return false; }
		   if (obj == this) { return true; }
		   if (obj.getClass() != getClass()) {
		     return false;
		   }
		   TermMap otherTm = (TermMap) obj;
		   if(getExpressions().size()!=otherTm.getExpressions().size()){
			   return false;
		   }
		   
		   EqualsBuilder eqb =  new EqualsBuilder();
		                 
		   for(int i = 0; i< getExpressions().size(); i++){
			   eqb.append(getExpressions().get(i).toString(), otherTm.getExpressions().get(i).toString());
		   }
		                 
		   return eqb.isEquals();

	}


	public Expression getLiteralValBinary() {
		return literalValBinary;
	}
	
	public Expression getLiteralValDate() {
		return literalValDate;
	}
	
	public Expression getLiteralValNumeric() {
		return literalValNumeric;
	}
	
	public Expression getLiteralValString() {
		return literalValString;
	}
	
	public Expression getLiteralLang() {
		return literalLang;
	}
	
	public Expression getLiteralType() {
		return literalType;
	}
	
	public Expression getTermType() {
		return termType;
	}
	
	
	public List<Expression> getLiteralVals(){
	  
	  return Arrays.asList(getLiteralValBinary(), getLiteralValBool(), getLiteralValString(), getLiteralValDate(), getLiteralValNumeric());
	}
	
	
	/**
	 * Creates an Expression that returns, if the Term represented here returns null.
	 * 
	 * In case of all null statements, it will return constant false,
	 * in case of constant expressions, it will return constant true; 
	 * @return
	 */
	public Expression getNotNullExpression(){
	  
	  List<Expression> toChecks = new ArrayList<Expression>( getLiteralVals());
	  toChecks.addAll(getResourceColSeg());
	  
	 
	  List<Expression> isNotNulls = new ArrayList<Expression>();
	  for(Expression tocheck: toChecks){
	    IsNullExpression inn = new IsNullExpression();
	    inn.setNot(true);
	    inn.setLeftExpression(tocheck);
	    isNotNulls.add(inn );
	  }
	  
	  
	
	  
	 
	  
	  
	  
	  return  new Parenthesis(FilterUtil.disjunct(isNotNulls));
	  
	  
	}
	

	
}
