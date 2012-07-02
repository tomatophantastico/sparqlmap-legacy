package org.aksw.sparqlmap.config.syntax.r2rml.dump;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.config.syntax.ColumDefinition;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

/**
 * Term Maps created out of a set of columns either out of one or multiple fromItems (for example created by join)
 * @author joerg
 * 
 */
public class TermMapColumn extends TermMap{

	

	
	/**
	 * a resource producing term
	 * @param dataTypeHelper
	 * @param expressions
	 */
	protected TermMapColumn(DataTypeHelper dataTypeHelper, List<Expression> expressions, List<FromItem> fromItems, List<EqualsTo> joinConditions, TripleMap trm) {
		super(dataTypeHelper,trm);		
		this.expressions = expressions;
		for(FromItem fi: fromItems){
			alias2fromItem.put(fi.getAlias(), fi);
		}
		this.joinConditions = joinConditions;
		//expressions = new ArrayList<Expression>();
	
	}
	
//	/**
//	 * a term, creating a literal
//	 * @param dataTypeHelper
//	 * @param literalType
//	 * @param expressions
//	 */
//	protected TermMapColumn(DataTypeHelper dataTypeHelper,List<Expression> expressions,List<FromItem> fromItems, List<EqualsTo> joinConditions, TripleMap trm) {
//		super(dataTypeHelper,trm);		
//		this.dataTypeHelper = dataTypeHelper;
//		this.expressions = expressions;
//		for(FromItem fi: fromItems){
//			alias2fromItem.put(fi.getAlias(), fi);
//		}
//		this.joinConditions = joinConditions;
//		
//	}
//	
	
	@Override
	public List<Expression> getExpressions() {
		
		return expressions;
	}

	public ColumDefinition getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(ColumDefinition createdBy) {
		this.createdBy = createdBy;
	}

	public TermMapColumn getOriginal() {
		return original;
	}

	public void setOriginal(TermMapColumn original) {
		this.original = original;
	}

	



}


//
//private List<SelectExpressionItem> getResourceSelectExpressionItems(List<Expression> expressions ,
//		String colalias) {
//	//get all resource
//	List<SelectExpressionItem> seis = new ArrayList<SelectExpressionItem>();
//	if(expressions!=null){
//	for (int i = 0; i < expressions.size(); i++) {
//		SelectExpressionItem sei = new SelectExpressionItem();
//		sei.setAlias(colalias + ColumnHelper.RESOURCE_COL_SEGMENT + i);
//		if(expressions.get(i) instanceof StringValue){
//			sei.setExpression(FilterUtil.cast(expressions.get(i), this.dataTypeHelper.getStringCastType()));
//		}else{
//		sei.setExpression(expressions.get(i));
//		}
//		seis.add(sei);
//	}}
//
//	return seis;
//}
//public List<SelectExpressionItem> getSelectExpressionItems(ColumDefinition coldef, String colalias) {
//	List<SelectExpressionItem> seis = new ArrayList<SelectExpressionItem>();
//	seis.addAll(getResourceSelectExpressionItems(coldef.getTerms(), colalias));
//	seis.addAll(getLiteralSelectExrpessionItems(coldef.getTerms(),colalias));
//	return seis;
//}
//private List<SelectExpressionItem> getLiteralSelectExrpessionItems(List<Expression> terms,
//		String colalias) {
//
//	List<SelectExpressionItem> seis = new ArrayList<SelectExpressionItem>();
//	
//	
//	for(Expression term: terms){
//		String type = FilterUtil.getCastType(term);
//
//		if (dataTypeHelper.getDateCastType().equals(type)) {
//			SelectExpressionItem l_date = new SelectExpressionItem();
//			l_date.setAlias(colalias + ColumnHelper.LITERAL_COL_DATE);
//			l_date.setExpression(term);
//			seis.add(l_date);
//		}
//		if (dataTypeHelper.getDateCastType().equals(type)) {
//			SelectExpressionItem l_numeric = new SelectExpressionItem();
//			l_numeric.setAlias(colalias + ColumnHelper.LITERAL_COL_NUM);
//			l_numeric.setExpression(term);
//			seis.add(l_numeric);
//		}
//		if(dataTypeHelper.getStringCastType().equals(type)){
//		SelectExpressionItem l_string = new SelectExpressionItem();
//		l_string.setAlias(colalias + ColumnHelper.LITERAL_COL_STRING);
//
//		l_string.setExpression(term);
//		seis.add(l_string);
//		}	
//	}
//	return seis;
//
//}

