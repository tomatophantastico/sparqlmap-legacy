package org.aksw.sparqlmap.core.mapper.translate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.UnionOp;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

public class UnionWrapper implements Wrapper {
	
	
	private DataTypeHelper dth;
    
	
	private boolean aligned = false;
	private TreeMap<String, SelectExpressionItem> seiTreeMap = new TreeMap<String, SelectExpressionItem>();
	private SetOperationList union;
	private Set<String> variablesMentioned = new HashSet<String>();
	private BiMap<String, TermMap> var2termMap;


	private SubSelect subselect;

	/**
	 * creates a new sql Union and registers it the the wrapper2body map
	 * @param selectBody2Wrapper
	 */
	public UnionWrapper( DataTypeHelper dth) {
		this.dth = dth;
		this.union = new SetOperationList();
		union.setOpsAndSelects(new ArrayList<PlainSelect>(),new ArrayList<SetOperation>());
	}



	public void addPlainSelectWrapper(PlainSelectWrapper plainSelect) {
		
	
		List<SelectItem> seis = plainSelect.getPlainSelect().getSelectItems();
		
		for (SelectItem selectExpressionItem : seis) {
			if(selectExpressionItem instanceof SelectExpressionItem){
				seiTreeMap.put(((SelectExpressionItem)selectExpressionItem).getAlias(), (SelectExpressionItem) selectExpressionItem);
			}else{
				throw new ImplementationException("non-explicit select item used");
			}
			
		}
		
		//for tp
		union.getPlainSelects().add(plainSelect.getPlainSelect());
		UnionOp uop = new UnionOp();
		uop.setAll(true);
		union.getOperations().add(uop);

		variablesMentioned.addAll(plainSelect.getVarsMentioned());
	}

	public SetOperationList getUnion() {
		return union;
	}

	@Override
	public SelectBody getSelectBody() {
		
		if(!aligned){
			align();
			aligned=true;
		}
		
		
		

		return union;
	}

		/**
		 * ensures that all unions have the same number of cols.
		 * the treemap filled while adding plainselects is used to check if the query contains
		 * all variables. if not null cols are added.
		 * 
		 */
	private void align() {
				
		 for(Object obj: union.getPlainSelects()){
				PlainSelect ps  = (PlainSelect) obj;
				List<SelectItem> filledUp = new ArrayList<SelectItem>();
				Map<String,SelectExpressionItem> alias2sei4ps = new HashMap<String, SelectExpressionItem>();
				for(Object o_sei: ps.getSelectItems()){
					SelectExpressionItem sei  = (SelectExpressionItem) o_sei;
					alias2sei4ps.put(sei.getAlias(), sei);
				}
				for(String alias : this.seiTreeMap.keySet()){
					if(alias2sei4ps.containsKey(alias)){
						filledUp.add(alias2sei4ps.get(alias));
					}else{
						String origCastType = DataTypeHelper.getCastType(seiTreeMap.get(alias).getExpression());
						
						SelectExpressionItem emptysei = new SelectExpressionItem();
						emptysei.setAlias(alias);
						
						//if part of a resource, than use an empty string, otherwise use 
						if(ColumnHelper.isColnameResourceSegment(alias)){
							emptysei.setExpression(dth.cast(new StringValue("\"\""),dth.getCastType(alias)));
						}else{
							emptysei.setExpression(dth.cast(new NullValue(),dth.getCastType(alias)));
						}
						filledUp.add(emptysei);

					}
				}
				ps.setSelectItems(filledUp);
				
				
		 }
	}
	

	
	public BiMap<String,TermMap> getVar2TermMap(String subselectName){
		
		
		if(var2termMap==null){
			createMappingCols(subselectName);
		}

		return this.var2termMap;
		
	}
	
	
	private void createMappingCols(String subselectName){
		
		
		//bucket the expression by variable
		
		
		
		Multimap<String,Expression> var2siExpression = LinkedHashMultimap.create();
		
		for(String colname: seiTreeMap.keySet()){
			String var = ColumnHelper.colnameBelongsToVar(colname);
			SelectExpressionItem sei  = seiTreeMap.get(colname);
			var2siExpression.put(var, ColumnHelper.createColumn(subselectName, sei.getAlias()));

		}
		this.var2termMap =  HashBiMap.create();
		
		this.subselect = new SubSelect();
		subselect.setAlias(subselectName);
		subselect.setSelectBody(union);
		
		for(String var : var2siExpression.keySet()){
			TermMap tm = TermMap.createTermMap(dth, var2siExpression.get(var));
			tm.addFromItem(subselect);
			this.var2termMap.put(var, tm);
		}
	}
	
	

	@Override
	public Set<String> getVarsMentioned() {
		return variablesMentioned;
	}
	
	
	@Override
	public List<SelectItem> getSelectExpressionItems() {
		return new ArrayList<SelectItem>(seiTreeMap.values());
		
	}

}
