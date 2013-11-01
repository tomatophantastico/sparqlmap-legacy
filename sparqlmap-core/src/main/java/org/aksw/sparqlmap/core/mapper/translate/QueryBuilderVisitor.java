package org.aksw.sparqlmap.core.mapper.translate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByExpressionElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.core.mapper.finder.MappingBinding;
import org.aksw.sparqlmap.core.mapper.finder.QueryInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.Sets;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
public class QueryBuilderVisitor extends OpVisitorBase {
	
	
	private DataTypeHelper dataTypeHelper;
	private ExpressionConverter exprconv;
	//defines if the filters should be pushed into the unions
	private boolean pushFilters = true;
	

	private static Logger log = LoggerFactory
			.getLogger(QueryBuilderVisitor.class);
	
	private Map<SelectBody, Wrapper> selectBody2Wrapper = new HashMap<SelectBody, Wrapper>();
	private Stack<SelectBody> selects = new Stack<SelectBody>();
	
	TermMap crc;

	private FilterUtil filterUtil;
	private final TranslationContext translationContext;

	public QueryBuilderVisitor(TranslationContext translationContext,	DataTypeHelper dataTypeHelper, ExpressionConverter expressionConverter, FilterUtil filterUtil) {
		this.filterUtil = filterUtil;
		this.dataTypeHelper = dataTypeHelper;
		this.exprconv = expressionConverter;
		this.translationContext = translationContext;
	}

	@Override
	public void visit(OpUnion opUnion) {

		
		
		PlainSelectWrapper ps1 = (PlainSelectWrapper) selectBody2Wrapper
				.get(selects.pop());
		PlainSelectWrapper ps2 = (PlainSelectWrapper) selectBody2Wrapper
				.get(selects.pop());
		
		if(ps1.getVar2TermMap().isEmpty()&& ps2.getVar2TermMap().isEmpty()){
			log.error("For union, both queries are empty. This rather problematic");
			selects.push(ps1.getSelectBody());
			
		}else if (ps1.getVar2TermMap().isEmpty()){
			log.info("found empty select for union, skipping it");
			selects.push(ps2.getSelectBody());
			
		}else if(ps2.getVar2TermMap().isEmpty()){
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
	public void visit(OpTable opTable){
		selects.push(new DummyBody());

	}
	
	public static class DummyBody implements SelectBody{

		@Override
		public void accept(SelectVisitor selectVisitor) {
			
		}
		
	}
	
	
	@Override
	public void visit(OpGraph opGraph) {
		log.error("Implement OpGraph");
	}

	@Override
	public void visit(OpLeftJoin opLeftJoin) {

		
		SelectBody mainsb = selects.pop();
		PlainSelectWrapper main = null;
		if(! (mainsb instanceof DummyBody)){
			main = (PlainSelectWrapper) selectBody2Wrapper
					.get(mainsb);
		}
		
		SelectBody leftsb =selects.pop();
		PlainSelectWrapper left = (PlainSelectWrapper) selectBody2Wrapper
					.get(leftsb);
	
		
		if(left.getVar2TermMap().isEmpty()){
			log.warn("left is empty");
		}else if(main ==null){
			main = left;
			main.setOptional(true);
			
		}else{
			main.addSubselect(left,true);
		}

		
//        if(!selects.isEmpty()){
//        	SelectBody stackUnder = selects.pop();
//    		selects.push(main.getSelectBody());
//    		selects.push(stackUnder);
//        }else{
//
//        }
		selects.push(main.getSelectBody());

	}
	
	
	@Override
	public void visit(OpFilter opfilter){
		PlainSelectWrapper wrap  = (PlainSelectWrapper) this.selectBody2Wrapper.get(selects.peek());
		
		if(this.pushFilters == true && wrap.getSubselects().size()>0){
			// try to stuff everything into the unions
			for(Expr toPush : opfilter.getExprs().getList()){
				
				Set<String> filterVars = new HashSet<String>();
				for(Var var: toPush.getVarsMentioned()){
					filterVars.add(var.getName());
				}
			
				
				
				boolean unpushable = false;
				for(SubSelect subselect :wrap.getSubselects().keySet()){
					Wrapper subSelectWrapper = wrap.getSubselects().get(subselect);
					if(subSelectWrapper instanceof UnionWrapper){
						//do that now for all plainselects of the union
						for(PlainSelect ps: ((UnionWrapper)subSelectWrapper).getUnion().getPlainSelects()){
							PlainSelectWrapper psw = (PlainSelectWrapper) selectBody2Wrapper.get(ps);
							Set<String> pswVars = psw.getVarsMentioned();
							if(pswVars.containsAll(filterVars)){
								// if all filter variables are covered by the triples of the subselect, it can answer it.
								psw.addFilterExpression(Arrays.asList(toPush));
							}else if(Collections.disjoint(filterVars, pswVars)){
								//if none are shared, than this filter simply does not matter for this wrapper 
							}else{
								//if there only some variables of the filter covered by the wrapper, answering in the top opration is neccessary.
								unpushable = true;
							}
							

						}
						
						
					}else{
						// do nothing else, here be dragons
						unpushable = true;
					}
				}
				if(unpushable){
					//so the filter could not be pushed, we need to evaluate in the upper select
					wrap.addFilterExpression(new ArrayList<Expr>(Arrays.asList(toPush)));
				}
			}
			
			
			
			
		}else{
			//no filter pushing, just put it in
			
			wrap.addFilterExpression(new ArrayList<Expr>(opfilter.getExprs().getList()));
		}
		
		
		
		
		
	
		
	}
	

	@Override
	public void visit(OpBGP opBGP) {

		PlainSelectWrapper bgpSelect = new PlainSelectWrapper(selectBody2Wrapper,dataTypeHelper,exprconv,filterUtil, translationContext);

		// PlainSelect bgpSelect = new PlainSelect();

		// for each triple we either calculate a from item, that is either one
		// column
		// of a table or a subselect if more than one column of a table were
		// applicable or we add the col directly to the bgpSelect

		for (Triple triple : opBGP.getPattern().getList()) {
			addTripleBindings(bgpSelect,triple,false);
		}

		this.selects.push(bgpSelect.getSelectBody());

		// TODO Auto-generated method stub
		super.visit(opBGP);
	}
	
	private Wrapper addTripleBindings( PlainSelectWrapper psw, Triple triple, boolean isOptional) {

		
		
		Collection<TripleMap> trms = translationContext.getQueryBinding().getBindingMap().get(triple);
		

		// do we need to create a union?
		if(trms.size()==1&&trms.iterator().next().getPos().size()==1&&filterUtil.getOptConf().optimizeSelfJoin){
			TripleMap trm = trms.iterator().next();
			PO po = trm.getPos().iterator().next();
			//no we do not need
			psw.addTripleQuery(trm.getSubject(), triple
					.getSubject().getName(), po.getPredicate(),triple
					.getPredicate().getName(),po.getObject(),triple.getObject().getName(), isOptional);
			if(translationContext.getQueryInformation().isProjectionPush()){
				//psw.setDistinct(true);
				psw.setLimit(1);
			}
	
			
			return psw;
		}else if(trms.size()==0){
			// no triple maps found.
			//bind to null values instead.
			psw.addTripleQuery(TermMap.createNullTermMap(dataTypeHelper), triple
					.getSubject().getName(), TermMap.createNullTermMap(dataTypeHelper),triple
					.getPredicate().getName(),TermMap.createNullTermMap(dataTypeHelper),triple.getObject().getName(), isOptional);
			
			return psw; 
			
		}else{
			List<PlainSelectWrapper> pselects = new ArrayList<PlainSelectWrapper>();

			//multiple triple maps, so we contruct a union
			
			
			for (TripleMap trm : trms) {
				for (PO po : trm.getPos()) {

					PlainSelectWrapper innerPlainSelect = new PlainSelectWrapper(this.selectBody2Wrapper,dataTypeHelper,exprconv,filterUtil, translationContext);
					//build a new sql select query for this pattern
					innerPlainSelect.addTripleQuery(trm.getSubject(), triple
							.getSubject().getName(), po.getPredicate(),triple
							.getPredicate().getName(),po.getObject(),triple.getObject().getName(), isOptional);
					if(translationContext.getQueryInformation().isProjectionPush()){
						//innerPlainSelect.setDistinct(true);
						innerPlainSelect.setLimit(1);
					}
					pselects.add(innerPlainSelect);
					
				}
			}
			
			UnionWrapper union = new UnionWrapper(this.selectBody2Wrapper,dataTypeHelper);
			for (PlainSelectWrapper plainSelectWrapper : pselects) {
				union.addPlainSelectWrapper(plainSelectWrapper);

			}
			
			psw.addSubselect(union,isOptional);
			return union;
		}
		
		
		
		
		
		
		
		
		
	

	}

	
	public void visit(OpJoin opJoin) {
		
		SelectBody left = this.selects.pop();
		SelectBody right = this.selects.pop();
		
		PlainSelectWrapper leftWrapper = (PlainSelectWrapper) this.selectBody2Wrapper.get(left);
		PlainSelectWrapper rightWrapper = (PlainSelectWrapper) this.selectBody2Wrapper.get(right);
		leftWrapper.addSubselect(rightWrapper, false);
		this.selects.push(left);
		
		
		
	}
	
	
	
	
	
	public Select getSqlQuery() {
		if (this.selects.size() != 1) {
			throw new RuntimeException("Stack not stacked properly");
		}

		// we need to enforce projection and slicing & ordering

		SelectBody sb = selects.pop();
		PlainSelect toModify = null;

		BiMap<String, TermMap> var2termMap;
		
		if(sb instanceof SetOperationList){
			
		
			PlainSelectWrapper wrap = new PlainSelectWrapper(selectBody2Wrapper,dataTypeHelper,exprconv,filterUtil, translationContext);
			
			wrap.addSubselect(this.selectBody2Wrapper
					.get(sb), false);
			sb = wrap.getSelectBody();
		}
		
		
//		if (sb instanceof PlainSelect) {
			toModify = (PlainSelect) sb;
			
			List<SelectExpressionItem> removeseis = new ArrayList<SelectExpressionItem>();
			
			List<String> projectVars = new ArrayList<String>();
			
			for(Var var: translationContext.getQueryInformation().getProject()
					.getVars()){
				projectVars.add(var.getName());
			}
					

			for(Object o_sei:  ((PlainSelect) sb).getSelectItems()){
				SelectExpressionItem sei = (SelectExpressionItem) o_sei;
				String varname = ColumnHelper.colnameBelongsToVar(sei.getAlias());
				//eventuall clean from deunion
				if(varname.contains("-du")){
					varname = varname.substring(0, varname.length()-5);
			    }
				
				if(!projectVars.contains(varname)){
					removeseis.add(sei);
				}
			}
			
			((PlainSelect) sb).getSelectItems().removeAll(removeseis);
			
			

			var2termMap = ((PlainSelectWrapper) selectBody2Wrapper.get(sb)).getVar2TermMap();


		if (translationContext.getQueryInformation().getOrder() != null && toModify.getOrderByElements() == null) {
			// if the list is not set, we create a new set
			List<OrderByElement> obys = exprconv.convert(
					translationContext.getQueryInformation().getOrder(), var2termMap);
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

		if (translationContext.getQueryInformation().getSlice() != null && toModify.getLimit() == null) {
			
			
			toModify = dataTypeHelper.slice(toModify,translationContext.getQueryInformation().getSlice());
			

		}
		
		
		if(translationContext.getQueryInformation().getDistinct()!=null){
			toModify.setDistinct(new Distinct());
		}

		Select select = new Select();
		select.setSelectBody(toModify);

		return select;
	}
	


}
