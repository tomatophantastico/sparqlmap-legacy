package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByExpressionElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.Union;

import org.aksw.sparqlmap.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml.MappingFilterFinder;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml.PlainSelectWrapper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml.Binding;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml.ScopeBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;

public class QueryBuilderVisitor extends OpVisitorBase {

	private static Logger log = LoggerFactory
			.getLogger(QueryBuilderVisitor.class);

	private R2RMLModel mappingConfiguration;
	private Binding queryBinding;
	private FilterUtil filterUtil;

	private Map<SelectBody, Wrapper> selectBody2Wrapper = new HashMap<SelectBody, Wrapper>();
	private Stack<SelectBody> selects = new Stack<SelectBody>();
	
	TermMap crc;


	private DataTypeHelper dataTypeHelper;

	private MappingFilterFinder mappingFilterFinder;

	public QueryBuilderVisitor(R2RMLModel mappingConfiguration,
			MappingFilterFinder mff, Binding queryBinding, DataTypeHelper dataTypeHelper, FilterUtil filterUtil) {
		this.mappingConfiguration = mappingConfiguration;
		this.queryBinding = queryBinding;
		this.dataTypeHelper =dataTypeHelper;
		this.filterUtil = filterUtil;
		this.mappingFilterFinder= mff;
	}

	@Override
	public void visit(OpUnion opUnion) {

		
		
		PlainSelectWrapper ps1 = (PlainSelectWrapper) selectBody2Wrapper
				.get(selects.pop());
		PlainSelectWrapper ps2 = (PlainSelectWrapper) selectBody2Wrapper
				.get(selects.pop());
		
		if(ps1.getColstring2Var().isEmpty()&& ps2.getColstring2Var().isEmpty()){
			log.error("For union, both queries are empty. This rather problematic");
			selects.push(ps1.getSelectBody());
			
		}else if (ps1.getColstring2Var().isEmpty()){
			log.info("found empty select for union, skipping it");
			selects.push(ps2.getSelectBody());
			
		}else if(ps2.getColstring2Var().isEmpty()){
			log.info("found empty select for union, skipping it");
			selects.push(ps1.getSelectBody());
		}else{
			UnionWrapper union = new UnionWrapper(selectBody2Wrapper);
			union.addPlainSelectWrapper(ps1);
			union.addPlainSelectWrapper(ps2);
			selects.push(union.getSelectBody());
		}
		
		
		
		
		

		

	}

	@Override
	public void visit(OpLeftJoin opLeftJoin) {

		PlainSelectWrapper left = (PlainSelectWrapper) selectBody2Wrapper
				.get(selects.pop());
		PlainSelectWrapper main = (PlainSelectWrapper) selectBody2Wrapper
				.get(selects.pop());
		
		if(left.getColstring2Var().isEmpty()){
			log.warn("left is empty");
		}else{
			main.addSubselect(left,true);
		}

		

		selects.push(main.getSelectBody());
	}
	
	
	@Override
	public void visit(OpFilter opfilter){
		PlainSelectWrapper wrap  = (PlainSelectWrapper) this.selectBody2Wrapper.get(selects.peek());
		List<Expr> filters = opfilter.getExprs().getList();
		List<Expr> realFilterS= new ArrayList<Expr>();
		for(Expr filter: filters){
			//we ignore all the filters introduced 
			//by the beautifier here, as this type of filter gets processed earlier on
			
			
//			if (filter instanceof E_Equals) {
//				E_Equals e_e = (E_Equals) filter;
//				
//				if (e_e.getArg1() instanceof ExprVar && e_e.getArg2() instanceof NodeValue) {
//					ExprVar leftVar = (ExprVar) e_e.getArg1();
//					NodeValue rightVar = (NodeValue) e_e.getArg2();
//					if(leftVar.asVar().getVarName().endsWith(ColumnHelper.COL_NAME_INTERNAL)){
//						break;
//					}
//
//				}
//				
//			}
			realFilterS.add(filter);
		}
		
		
		
		wrap.addFilterExpression(realFilterS);
	}
	

	@Override
	public void visit(OpBGP opBGP) {

		PlainSelectWrapper bgpSelect = new PlainSelectWrapper(selectBody2Wrapper, mappingConfiguration, dataTypeHelper);

		// PlainSelect bgpSelect = new PlainSelect();

		// for each triple we either calculate a from item, that is either one
		// column
		// of a table or a subselect if more than one column of a table were
		// applicable or we add the col directly to the bgpSelect

		for (Triple triple : opBGP.getPattern().getList()) {

			// first we determine, whether the we need to create a subselect or
			// of
			// the ?p can be mapped to a single column

			// we know, that p is a variable, as we previously ran the
			// prettifier

		
			
			
			
			bgpSelect.addSubselect(generateUnionFromItem(bgpSelect,triple), false);
			
			
//			if (pUri != null) {
//				// pUri is given.
//
//				if (sbmap.getLdps().size() == 1) {
//					// we can map everything in a simple query. We do that now
//					generateUnionFromItem(bgpSelect, triple, scope, sbmap);
//
//				} else if (sbmap.getLdps().size() == 0) {
//					log.error("Not working with unmappable triples yet.");
//					log.error("Could not map: " + triple.toString());
//				} else {
//					// p is given, but we could not map every triple in a
//					// straight way
//					generateUnionFromItem(bgpSelect,triple, scope, sbmap);
//				}
//
//			} else {
//				// p is not given.
//						
//				
//			}

		}

		this.selects.push(bgpSelect.getSelectBody());

		// TODO Auto-generated method stub
		super.visit(opBGP);
	}
	

//	/**
//	 * This function maps the triple with the varMapping into the bgpselect This
//	 * function works only, if the triples predicate is set and varmapping maps
//	 * to only one ldp
//	 * 
//	 * @param bgpSelect
//	 * @param triple
//	 * @param pUri
//	 * @param varMappingMap
//	 */
//	private void processQueryPattern(PlainSelectWrapper bgpSelect,
//			Triple triple, String pUri, ScopeBlock scope,SBlockNodeMapping sbmap) {
//		// add to the from or union
//		String ldp = sbmap.getLdps().iterator().next();
//
//		Set<Mapping> ldpMappings = new HashSet<Mapping>(sbmap.getMappings(ldp));
//
//		for (Mapping map : ldpMappings) {
//
//			// mapping contains columns that can be mapped for this triple
//			Collection<ColumDefinition> cols = map
//					.getColumDefinition(pUri);
//			if (cols != null && cols.size()>0) {
//				if (cols.size() == 1) {
//					// add the subject column
//					bgpSelect.addTripleQuery(
//							map.getIdColumn().getTermCreator(), triple
//									.getSubject().getName(),cols.iterator().next().getTermCreator(), triple
//									.getObject().getName(),false);
//	
//			
//				} else {
//					throw new ImplementationException(
//							"Non Implemented Behavior: predicate does not map to a single column. Need to implement union here.");
//				}
//
//				bgpSelect.addFilterExpression(scope.getFilterFor(
//						Var.alloc(triple.getSubject().getName()),
//						Var.alloc(triple.getObject())));
//			}
//
//		}
//	}

	private Wrapper generateUnionFromItem( PlainSelectWrapper bgpSelect, Triple triple) {

		String p = triple.getPredicate().getName();
	
		// do we need to create a union?
		
		Collection<TripleMap> trms = queryBinding.getBinding().get(triple);
		
		List<PlainSelectWrapper> pselects = new ArrayList<PlainSelectWrapper>();
		
		for (TripleMap trm : trms) {
			for (PO po : trm.getPos()) {

				PlainSelectWrapper plainSelect = new PlainSelectWrapper(this.selectBody2Wrapper,mappingConfiguration,dataTypeHelper);
				
				

				// add subject line
				plainSelect.addTripleQuery(trm.getSubject(), triple
						.getSubject().getName(), po.getPredicate(),triple
						.getPredicate().getName(),po.getObject(),triple.getObject().getName(), false);

				if(trm.getSubject().getFromItems().iterator().next().getAlias().contains("query")){
					plainSelect.getClass();
				}
				
				
				pselects.add(plainSelect);
				

//				plainSelect.addFilterExpression(queryBinding.getFilter(
//									Var.alloc(triple.getSubject().getName()),
//									Var.alloc(triple.getPredicate().getName()),
//									Var.alloc(triple.getObject().getName())));
			}
			
		}
		
		
		
		
		
		
		if(pselects.size()==1){
			return pselects.iterator().next();
		}else{
			UnionWrapper union = new UnionWrapper(this.selectBody2Wrapper);
			for (PlainSelectWrapper plainSelectWrapper : pselects) {
				union.addPlainSelectWrapper(plainSelectWrapper);

			}
			return union;
		}

	}

	
	
	
	
	public Select getSqlQuery() {
		if (this.selects.size() != 1) {
			throw new RuntimeException("Stack not stacked properly");
		}

		// we need to enforce projection and slicing & ordering

		SelectBody sb = selects.pop();
		PlainSelect toModify = null;

		BiMap<String, String> colstring2var;
		Map<String, TermMap> colstring2col;
		
		if(sb instanceof Union){
	PlainSelectWrapper wrap = new PlainSelectWrapper(selectBody2Wrapper, mappingConfiguration,dataTypeHelper);
			
			wrap.addSubselect(this.selectBody2Wrapper
					.get(sb), false);
			sb = wrap.getSelectBody();
		}
		
		
//		if (sb instanceof PlainSelect) {
			toModify = (PlainSelect) sb;
			
			List<SelectExpressionItem> removeseis = new ArrayList<SelectExpressionItem>();
			
			List<String> projectVars = new ArrayList<String>();
			
			for(Var var: mappingFilterFinder.getProject()
					.getVars()){
				projectVars.add(var.getName());
			}
					

			for(Object o_sei:  ((PlainSelect) sb).getSelectItems()){
				SelectExpressionItem sei = (SelectExpressionItem) o_sei;
				String varname = ColumnHelper.colnameBelongsToVar(sei.getAlias());
				if(!projectVars.contains(varname)){
					removeseis.add(sei);
				}
			}
			
			((PlainSelect) sb).getSelectItems().removeAll(removeseis);
			
			

			colstring2var = ((PlainSelectWrapper) selectBody2Wrapper.get(sb)).getColstring2Var();
			colstring2col = ((PlainSelectWrapper) selectBody2Wrapper.get(sb)).getColstring2Col();


		if (mappingFilterFinder.getOrder() != null && toModify.getOrderByElements() == null) {
			// if the list is not set, we create a new set
			List<OrderByElement> obys = filterUtil.convert(
					mappingFilterFinder.getOrder(), colstring2var,colstring2col);
			int i = 0;
			for (OrderByElement orderByElement : obys) {
				
				
				if(orderByElement instanceof OrderByExpressionElement){
					//we check, if the appropriate expression is there.
					OrderByExpressionElement obx = (OrderByExpressionElement) orderByElement;
					Expression obExpr = obx.getExpression();
					Expression matchingExp = null;
					for (Object osei : toModify.getSelectItems()) {
						SelectExpressionItem sei  = (SelectExpressionItem) osei; 
						if(sei.getExpression().toString().equals(obExpr.toString())){
							matchingExp = sei.getExpression();
							break;
						}
						
					}
					
					if(matchingExp==null){
						//we need to add the order by column to the select item
						SelectExpressionItem obySei = new SelectExpressionItem();
						obySei.setAlias(ColumnHelper.COL_NAME_LITERAL_NUMERIC + i++);
						obySei.setExpression(obExpr);
						toModify.getSelectItems().add(obySei);
					}
				}
				
			}
			
			
			toModify.setOrderByElements(obys);
		}

		if (mappingFilterFinder.getSlice() != null && toModify.getLimit() == null) {
			Limit limit = new Limit();
			if (mappingFilterFinder.getSlice().getStart() >= 0) {
				limit.setOffset(mappingFilterFinder.getSlice().getStart());
			}
			if (mappingFilterFinder.getSlice().getLength() >= 0) {
				limit.setRowCount(mappingFilterFinder.getSlice().getLength());
			}

			toModify.setLimit(limit);

		}
		
		
		if(mappingFilterFinder.getDistinct()!=null){
			toModify.setDistinct(new Distinct());
		}

		Select select = new Select();
		select.setSelectBody(toModify);

		return select;
	}
	


}
