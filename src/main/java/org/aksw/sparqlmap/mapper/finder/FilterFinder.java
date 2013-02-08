package org.aksw.sparqlmap.mapper.finder;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;

public class FilterFinder{
	
	
	
	
	
	public static QueryInformation getQueryInformation(Op op) {
		// for every triple, this map holds all expressions that are applicable
		// to its variables.
		QueryInformation qi = new QueryInformation();
		qi.setQuery(op);

		final Multiset<Expr> filterStack = HashMultiset.create();

		OpWalker.walk(op,new Putter(filterStack, qi), new Pusher(filterStack), 
				new Popper(filterStack));

		return qi;

	}
	

	
	
	/**
	 * fills the stack
	 * @author joerg
	 *
	 */
	private static class Pusher extends OpVisitorBase{
		Multiset<Expr> filterStack;
		public Pusher(Multiset<Expr> filterStack) {
			this.filterStack = filterStack;
		}

		@Override
		public void visit(OpFilter filter){
			for(Expr expr: filter.getExprs().getList()){
				filterStack.add(expr);
			}
			
		}
	}
	/**
	 * removes from the stack
	 * @author joerg
	 *
	 */
	private static class Popper extends OpVisitorBase{
		
		private Multiset<Expr> filterStack;

		public Popper(Multiset<Expr> filterStack) {
			this.filterStack = filterStack;		}

		@Override
		public void visit(OpFilter filter) {
			for(Expr expr: filter.getExprs().getList()){
				filterStack.remove(expr);
			}
		}
				
	}
	
	/**
	 * puts the filters into the triples2variables2expressions- map
	 * @author joerg
	 *
	 */
	private static class Putter extends OpVisitorBase{
		
		private Multiset<Expr> filterStack;
		private QueryInformation qi;

		public Putter(
				Multiset<Expr> filterStack,
				QueryInformation qi) {
			this.filterStack = filterStack;
			this.qi = qi;
		}
		@Override
		public void visit(OpDistinct opDistinct) {
			qi.setDistinct(opDistinct);
			
		}
		
		@Override
		public void visit(OpOrder opOrder) {
			qi.setOrder(opOrder);
		}
		
		@Override
		public void visit(OpProject opProject) {
			qi.setProject(opProject);
		}
		@Override
		public void visit(OpReduced opReduced) {
			qi.setReduced(opReduced);
		}
		
		@Override
		public void visit(OpSlice opSlice) {
			qi.setSlice(opSlice);
		}
		
		
		
		
		
		

		@Override
		public void visit(OpBGP opBGP) {
			
			for(Triple triple: opBGP.getPattern().getList()){
				
				Map<String,Collection<Expr>> var2expr = new HashMap<String, Collection<Expr>>();	
				Collection<Expr> sExprs = new HashSet<Expr>();
				Collection<Expr> pExprs = new HashSet<Expr>();
				Collection<Expr> oExprs = new HashSet<Expr>(); 
				
				for(Expr expr: filterStack){
					if(expr.getVarsMentioned().contains(Var.alloc(triple.getSubject().getName()))){
						sExprs.add(expr);
					}
					if(expr.getVarsMentioned().contains(Var.alloc(triple.getPredicate().getName()))){
						pExprs.add(expr);
					}
					if(expr.getVarsMentioned().contains(Var.alloc(triple.getObject().getName()))){
						oExprs.add(expr);
					}
				}
				
				var2expr.put(triple.getSubject().getName(), sExprs);
				var2expr.put(triple.getPredicate().getName(), pExprs);
				var2expr.put(triple.getObject().getName(), oExprs);
				
				qi.getFiltersforvariables().put(triple, var2expr);
				
				
			}
			

		}
		
		
	}
	

	
	
	
}
