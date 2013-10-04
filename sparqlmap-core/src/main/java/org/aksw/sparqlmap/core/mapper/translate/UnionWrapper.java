package org.aksw.sparqlmap.core.mapper.translate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.UnionOp;

import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;

import com.google.common.collect.BiMap;

public class UnionWrapper implements Wrapper {
	
	
	private DataTypeHelper dth;
    
	
	private boolean aligned = false;
	private TreeMap<String, SelectExpressionItem> seiTreeMap = new TreeMap<String, SelectExpressionItem>();
	private SetOperationList union;
	private Set<String> variablesMentioned = new HashSet<String>();
	private BiMap<String, String> colstring2var;
	private Map<String, TermMap> colstring2Col;

	/**
	 * creates a new sql Union and registers it the the wrapper2body map
	 * @param selectBody2Wrapper
	 */
	public UnionWrapper(Map<SelectBody, Wrapper> selectBody2Wrapper, DataTypeHelper dth) {
		this.dth = dth;
		this.union = new SetOperationList();
		union.setOpsAndSelects(new ArrayList<PlainSelect>(),new ArrayList<SetOperation>());
		selectBody2Wrapper.put(union, this);
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

		variablesMentioned.addAll(plainSelect.getColstring2Var().values());
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
	
	public Map<String,TermMap> getColString2Col(String subselectName){
		if(colstring2Col==null){
			createMappingCols(subselectName);
		}
		
		
		
		return colstring2Col;
	}
	
	public BiMap<String,String> getColString2Var(String subselectName){
		
		
		if(colstring2var==null){
			createMappingCols(subselectName);
		}

		return colstring2var;
		
	}
	
	
	private void createMappingCols(String subselectName){
		
		throw new ImplementationException("Rework to match R2RML Implementation");
		
		
//		colstring2Col = new HashMap();
//		colstring2var = HashBiMap.create();
//		Mapping mapp = new Mapping();
//	
//		mapp.setName(subselectName);
//		
//		Multimap<String, String> varname2col = LinkedListMultimap.create();
//		for (String  colname : this.seiTreeMap.keySet()) {
//			varname2col.put(ColumnHelper.colnameBelongsToVar(colname),colname);
//		}
//		
//		for(String varname: varname2col.keySet()){
//			//create the expressions backing the column
//			List<Expression> colExpressions = new ArrayList<Expression>();
//			for(String colname :varname2col.get(varname)){
//				colExpressions.add(MappingUtils.createCol(subselectName, colname));
//			}
//			MaterializedColumn col = new MaterializedColumn(colExpressions);
//			col.setSqldataType(1);
//			col.setMapp(mapp);
//			colstring2Col.put(col.toString(), col);
//			colstring2var.put(col.toString(),varname);
//		}
//		
		
		
				
		
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
