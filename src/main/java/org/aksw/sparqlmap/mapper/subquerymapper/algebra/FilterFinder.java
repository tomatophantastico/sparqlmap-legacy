package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.MappingConfiguration;

import com.google.common.collect.LinkedHashMultimap;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.Op0;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.Op2;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpN;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;

public class FilterFinder {
	
	MappingConfiguration conf;
	
	private Map<Op,Op> parentOfOp = new HashMap<Op, Op>();
	private LinkedHashMultimap<Op, Op> siblingsOfOp = LinkedHashMultimap.create();
	private Op root;

	protected OpDistinct opdistinct;

	protected OpProject opproject;

	protected OpSlice opslice;

	protected OpOrder oporder;

	

	public FilterFinder(MappingConfiguration conf, Op op) {
		super();
		this.conf = conf;
		root = op;
		analyzeFilters(op);
		analyzeModifiers(op);
	}
	
	
	
	private void analyzeModifiers(Op op) {
		
		OpWalker.walk(op, new OpVisitorBase(){
			

			@Override
			public void visit(OpDistinct opDistinct) {
				opdistinct = opDistinct;
				
			}
			
			
			@Override
			public void visit(OpProject opProject) {
				opproject = opProject;
				
			}
			
			@Override
			public void visit(OpSlice opSlice) {
				opslice = opSlice;
				
			}
			
			@Override
			public void visit(OpOrder opOrder) {
				oporder = opOrder;
				
			}
			
			
		});
		
		
	}



	/**
	 * returns a list of all filters that are applicable to that variable at this point in the parsed query tree
	 * will include multi-parameter variables, if and only if they are defined at this OP.
	 * 
	 * @param var
	 * @param in the OP on which level the transformation takes place
	 * @return
	 */
	public Set<Expr> getFilterForVariables(Op in, Var... var){
		
		Set<Var> vars = new HashSet<Var>(Arrays.asList(var));
		Set<Expr> expression = new HashSet<Expr>();
		
		expression.addAll(getFiltersDownFrom(in));
		expression.addAll(getFiltersUpFrom(in));
		
		Set<Expr> applicableVars = new HashSet<Expr>();
		
		for (Expr expr : expression) {
			if(vars.containsAll(expr.getVarsMentioned())){
				applicableVars.add(expr);
			}
		}
				
		return applicableVars;
	}
	
	/**
	 * checks if the variable at this point has an equals Uri statement
	 * @param var
	 * @param op
	 * @return
	 */
	public String getUri(Var var, Op op){
		Set<Expr> uriCand = getFilterForVariables(op, var);
		
		for (Expr expr : uriCand) {
			if(expr instanceof E_Equals){
				E_Equals ee = (E_Equals) expr;
				Expr arg1 = ee.getArg1();
				Expr arg2 = ee.getArg2();
				
				if(arg1.isVariable()&& arg2 instanceof NodeValueNode){
					Node_URI n_uri = (Node_URI) ((NodeValueNode) ee.getArg2()).asNode();
					//ee.asVar();
					return n_uri.getURI();
				}
				
				
				
			}
			
		}
		
		//throw new ImplementationException("not yet implemented");
		
		
		return null;
		
	}
	
	
	
	
	// check the siblings for filters
	// considering down: the root of the query is on top.
	private Collection<Expr> getFiltersDownFrom(Op here){
		Collection<Expr> exprs = new HashSet<Expr>();
		
		
		if(here instanceof OpFilter){
			OpFilter opFilter = (OpFilter) here;
			exprs.addAll(opFilter.getExprs().getList());
		}
		
		
		
		for (Op sibOp : siblingsOfOp.get(here)) {
			exprs.addAll(getFiltersDownFrom(sibOp));
		}
		
		
		return exprs;
	}
	
	// check the parents for filters
		private Collection<Expr> getFiltersUpFrom(Op here){
			Collection<Expr> exprs = new HashSet<Expr>();
			
			
			if(here instanceof OpFilter){
				OpFilter opFilter = (OpFilter) here;
				exprs.addAll(opFilter.getExprs().getList());
			}
			
			
			Op parent = parentOfOp.get(here);
			if(parent !=null){
				
				
			//we branch down in certain conditions
				
			if(here instanceof OpLeftJoin){
				OpLeftJoin opleft = (OpLeftJoin) here;
				exprs.addAll(getFiltersDownFrom(opleft.getLeft()));
				
			}
				
				
			
			exprs.addAll(getFiltersUpFrom(parent));
			}
			
			
			return exprs;
		}
	
	
	
	



	//filling the tables
	private void analyzeFilters(Op op){
		
		//create the tree here
		if(op instanceof Op1){
			Op1 op1 = (Op1) op;
			
			parentOfOp.put(op1.getSubOp(), op1);
			siblingsOfOp.put(op1, op1.getSubOp());
			analyzeFilters(op1.getSubOp());
			
		}else if (op instanceof Op2){
			Op2 op2 = (Op2) op;
			parentOfOp.put(op2.getLeft(), op2);
			parentOfOp.put(op2.getRight(), op2);
			siblingsOfOp.put(op2, op2.getLeft());
			siblingsOfOp.put(op2, op2.getRight());
			analyzeFilters(op2.getLeft());
			analyzeFilters(op2.getRight());
			
		}else if (op instanceof Op0){
			//do nothing here
			
		}else if (op instanceof OpN){
			OpN opn = (OpN) op;
			for(Op opnSibling: opn.getElements()){
				parentOfOp.put(opnSibling,opn);
				siblingsOfOp.put(opn,opnSibling);
				analyzeFilters(opnSibling);
			}
			
			
			
		}
		
	}
	
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer();
		str.append("Root: \n");
		str.append(root.toString());
		str.append("\n has parent map:");
		for (Op child : parentOfOp.keySet()) {
			str.append("child: ");
			str.append(child.getName());
			str.append(" has parent: ");
			str.append(parentOfOp.get(child).getName());
			str.append("\n");
		}
		
		for (Op parent : siblingsOfOp.keySet()) {
			str.append("parent: ");
			str.append(parent.getName());
			
			for (Op  sibling : siblingsOfOp.get(parent)) {
				str.append(" has sibling: ");
				str.append(sibling.getName());
				str.append("\n");
			}
			
			
		}
		str.append("Modifiers: \n");
		
		str.append(opdistinct + " \n****\n " + oporder+ " \n****\n " + opproject + " \n****\n " + opslice+ " \n****\n ");
		
		return str.toString();
	}



	public OpDistinct getOpdistinct() {
		return opdistinct;
	}



	public void setOpdistinct(OpDistinct opdistinct) {
		this.opdistinct = opdistinct;
	}



	public OpProject getOpproject() {
		return opproject;
	}



	public void setOpproject(OpProject opproject) {
		this.opproject = opproject;
	}



	public OpSlice getOpslice() {
		return opslice;
	}



	public void setOpslice(OpSlice opslice) {
		this.opslice = opslice;
	}



	public OpOrder getOporder() {
		return oporder;
	}



	public void setOporder(OpOrder oporder) {
		this.oporder = oporder;
	}

}
