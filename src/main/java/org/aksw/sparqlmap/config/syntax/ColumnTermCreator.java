package org.aksw.sparqlmap.config.syntax;

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

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

/**
 * This class contains the expressions behind a mapping
 * @author joerg
 * 
 */
public class ColumnTermCreator extends TermCreator{

	
	private ColumDefinition createdBy;
	
	private ColumnTermCreator original;
		
	/**
	 * the expressions that create a term.
	 */
	private List<Expression> expressions;
	
	
	private LinkedHashMap<String,FromItem> alias2fromItem = new LinkedHashMap<String, FromItem>();

	
	/**
	 * a resource producing term
	 * @param dataTypeHelper
	 * @param expressions
	 */
	protected ColumnTermCreator(DataTypeHelper dataTypeHelper, List<Expression> expressions, List<FromItem> fromItems, List<EqualsTo> joinConditions) {
		super(dataTypeHelper);		
		this.expressions = expressions;
		for(FromItem fi: fromItems){
			alias2fromItem.put(fi.getAlias(), fi);
		}
		this.joinConditions = joinConditions;
		//expressions = new ArrayList<Expression>();
	
	}
	
	/**
	 * a term, creating a literal
	 * @param dataTypeHelper
	 * @param literalType
	 * @param expressions
	 */
	protected ColumnTermCreator(DataTypeHelper dataTypeHelper,Integer literalType, List<Expression> expressions,List<FromItem> fromItems, List<EqualsTo> joinConditions) {
		super(dataTypeHelper);		
		this.dataTypeHelper = dataTypeHelper;
		this.expressions = expressions;
		for(FromItem fi: fromItems){
			alias2fromItem.put(fi.getAlias(), fi);
		}
		this.joinConditions = joinConditions;
		
	}
	
	
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

	public ColumnTermCreator getOriginal() {
		return original;
	}

	public void setOriginal(ColumnTermCreator original) {
		this.original = original;
	}

	
	@Override
	public TermCreator clone(final String suffix) {
		
		List<Expression> clonedExpressions = cloneColumnExpressions(this.expressions, suffix);
		
		//rename the from items Accordingly
		
		Collection<FromItem> origFromItems = this.alias2fromItem.values();
		
		final List<FromItem> clonedFromItems = cloneFromItems(suffix,
				origFromItems);
		
		
		Collection<EqualsTo> origjoinConditions = this.joinConditions;
		
		List<EqualsTo> clonedJoinConditions = cloneJoinsCondition(suffix,
				origjoinConditions);
		
		
		ColumnTermCreator clone = new ColumnTermCreator(this.dataTypeHelper, clonedExpressions,clonedFromItems,clonedJoinConditions);
		clone.setOriginal(this);
		
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
					throw new ImplementationException("clone of subselect view not supported yet.");
					
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

