package org.aksw.sparqlmap.mapper.translate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByExpressionElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;

import org.aksw.sparqlmap.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.mapper.finder.MappingBinding;
import org.aksw.sparqlmap.mapper.finder.QueryInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
public class QueryBuilderVisitor extends OpVisitorBase {
	
	
	private DataTypeHelper dataTypeHelper;
	private ColumnHelper columnhelper;
	ExpressionConverter exprconv;

	

	private static Logger log = LoggerFactory
			.getLogger(QueryBuilderVisitor.class);
	
	private MappingBinding queryBinding;

	private Map<SelectBody, Wrapper> selectBody2Wrapper = new HashMap<SelectBody, Wrapper>();
	private Stack<SelectBody> selects = new Stack<SelectBody>();
	
	TermMap crc;

	private QueryInformation mappingFilterFinder;
	private FilterOptimizer fopt;

	public QueryBuilderVisitor(	QueryInformation mff, MappingBinding queryBinding, DataTypeHelper dataTypeHelper, ExpressionConverter expressionConverter, ColumnHelper colhelper,FilterOptimizer fopt) {
		this.fopt = fopt;
		this.queryBinding = queryBinding;
		this.mappingFilterFinder= mff;
		this.dataTypeHelper = dataTypeHelper;
		this.exprconv = expressionConverter;
		this.columnhelper = colhelper;
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
			UnionWrapper union = new UnionWrapper(selectBody2Wrapper, dataTypeHelper);
			union.addPlainSelectWrapper(ps1);
			union.addPlainSelectWrapper(ps2);
			selects.push(union.getSelectBody());
		}
	}
	
	
	@Override
	public void visit(OpGraph opGraph) {
		PlainSelectWrapper wrap = (PlainSelectWrapper) selectBody2Wrapper.get(selects.peek());
		
		if(opGraph.getNode() instanceof Var){
			Var gvar = (Var) opGraph.getNode();
			
			List<SelectExpressionItem> newGraphSeis = new	ArrayList<SelectExpressionItem>();
//			newGraphSeis.addAll();
//			
//			ColumnHelper.getExpression(col, rdfType, sqlType, datatype, lang, lanColumn, dth, graph)
//			
//			new TermMap(dataTypeHelper, ColumnHelper.getBaseExpressions(ColumnHelper.COL_VAL_TYPE_RESOURCE, 2, ColumnHelper.COL_VAL_SQL_TYPE_RESOURCE, dataTypeHelper, null, null, null, null));
			
			
			
			//new Select Item, using the first 
			SelectExpressionItem graph_sei = new SelectExpressionItem();
			graph_sei.setAlias(gvar.getName() + ColumnHelper.COL_NAME_GRAPH);
			List<Expression> additionalFilters = new ArrayList<Expression>();
			
			for(SelectItem si : wrap.getSelectExpressionItems()){
				SelectExpressionItem sei  = (SelectExpressionItem) si;
				
				if(sei.getAlias().endsWith(ColumnHelper.COL_NAME_GRAPH) && sei.getExpression() instanceof Column){
					if(graph_sei.getExpression()==null){
						List<Expression> graphExprs =columnhelper.getExpression((Column)sei.getExpression(), ColumnHelper.COL_VAL_TYPE_RESOURCE, ColumnHelper.COL_VAL_SQL_TYPE_RESOURCE, null, null, null, dataTypeHelper, null,null);
						
						TermMap tm = new TermMap(dataTypeHelper, graphExprs);
						newGraphSeis.addAll(tm.getSelectExpressionItems(opGraph.getNode().getName()));
						
						
						
						graph_sei.setExpression(sei.getExpression());
					}else{
						IsNullExpression seiGraphIsNull = new IsNullExpression();
						seiGraphIsNull.setNot(false);
						seiGraphIsNull.setLeftExpression(graph_sei.getExpression());
						
						IsNullExpression thisGRaphIsNull = new IsNullExpression();
						thisGRaphIsNull.setNot(false);
						thisGRaphIsNull.setLeftExpression(sei.getExpression());
						
						AndExpression andNull = new AndExpression(seiGraphIsNull, thisGRaphIsNull);
						
						
						EqualsTo eq = new EqualsTo();
						
						eq.setLeftExpression(graph_sei.getExpression());
						eq.setRightExpression(sei.getExpression());
						
						
						OrExpression or = new OrExpression(new Parenthesis(andNull), new Parenthesis(eq));
						
						
						additionalFilters.add(or);
					}
				}
			}
			
			
			wrap.getSelectExpressionItems().addAll(newGraphSeis);
			
			
			wrap.addSQLFilter(FilterUtil.conjunctFilters(additionalFilters));
			
			
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
			realFilterS.add(filter);
		}
		
		
		
		wrap.addFilterExpression(realFilterS);
	}
	

	@Override
	public void visit(OpBGP opBGP) {

		PlainSelectWrapper bgpSelect = new PlainSelectWrapper(selectBody2Wrapper,dataTypeHelper,exprconv,fopt);

		// PlainSelect bgpSelect = new PlainSelect();

		// for each triple we either calculate a from item, that is either one
		// column
		// of a table or a subselect if more than one column of a table were
		// applicable or we add the col directly to the bgpSelect

		for (Triple triple : opBGP.getPattern().getList()) {
			
			
			bgpSelect.addSubselect(generateUnionFromItem(bgpSelect,triple), false);

		}

		this.selects.push(bgpSelect.getSelectBody());

		// TODO Auto-generated method stub
		super.visit(opBGP);
	}
	
	private Wrapper generateUnionFromItem( PlainSelectWrapper bgpSelect, Triple triple) {

		// do we need to create a union?
		
		Collection<TripleMap> trms = queryBinding.getBindingMap().get(triple);
		
		List<PlainSelectWrapper> pselects = new ArrayList<PlainSelectWrapper>();
		
		for (TripleMap trm : trms) {
			for (PO po : trm.getPos()) {

				PlainSelectWrapper plainSelect = new PlainSelectWrapper(this.selectBody2Wrapper,dataTypeHelper,exprconv,fopt);
				//build a new sql select query for this pattern
				plainSelect.addTripleQuery(trm.getSubject(), triple
						.getSubject().getName(), po.getPredicate(),triple
						.getPredicate().getName(),po.getObject(),triple.getObject().getName(), false);
				pselects.add(plainSelect);
				
			}
			
		}
		
		
		
		
		
		
		if(pselects.size()==1){
			return pselects.iterator().next();
		} else if (pselects.size()==0){
			throw new ImplementationException("Unmappable Triple (" + triple.toString()+") encountered. Implement the nohing match component");
			
		}else{
			UnionWrapper union = new UnionWrapper(this.selectBody2Wrapper,dataTypeHelper);
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
		
		if(sb instanceof SetOperationList){
	PlainSelectWrapper wrap = new PlainSelectWrapper(selectBody2Wrapper,dataTypeHelper,exprconv,fopt);
			
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
			List<OrderByElement> obys = exprconv.convert(
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
