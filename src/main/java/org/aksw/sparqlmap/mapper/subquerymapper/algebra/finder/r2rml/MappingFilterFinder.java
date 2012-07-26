package org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;



/**
 * Creates the query binding and collects some other useful information about the query.
 * 
 * @author joerg
 *
 */

public class MappingFilterFinder {

	private R2RMLModel mconf;
	private Op query;
	
	private OpSlice slice;
	private OpOrder order;
	private OpProject project;
	private OpDistinct distinct;
	private OpReduced reduced;

	public MappingFilterFinder(R2RMLModel mconf) {
		this.mconf = mconf;

	}

	/**
	 * we analyze the query top to bottom and group Ops (especially bgps) into
	 * Scope Blocks. These blocks share the same variable definition there.
	 * 
	 * we further merge all the triples an
	 * 
	 * @param query
	 * @return 
	 */
	public Binding createBindnings(Op query) {

		
		Binding queryBindning = createScopeBlocks(query);
		queryBindning.mapIt();
		return queryBindning;
		

	}
	
	

	private Binding createScopeBlocks(Op query) {
		
		
				
		if(query instanceof OpProject){
			this.project = ((OpProject) query);
			return createScopeBlocks(((OpProject) query).getSubOp());
		} else 	if(query instanceof OpReduced){
			this.reduced = ((OpReduced) query);
			return createScopeBlocks(((OpReduced) query).getSubOp());
		} else if (query instanceof OpSlice) {
			OpSlice slice = (OpSlice) query;
			this.slice = slice;
			return createScopeBlocks(((OpSlice) query).getSubOp());
			
		} else if (query instanceof OpOrder) {
			OpOrder order = (OpOrder) query;
			this.order = order;
			return createScopeBlocks(( (OpOrder) query).getSubOp());
			
		} else if(query instanceof OpDistinct){
			this.distinct = (OpDistinct) query;
			return createScopeBlocks(( (OpDistinct) query).getSubOp());
		} else  if (query instanceof OpFilter) {
			
			OpFilter filter = (OpFilter) query;
			
			
			
			Binding bind = createScopeBlocks(filter.getSubOp());
			bind.addFilter(filter.getExprs().getList());
			
			return bind;
			
		} else if (query instanceof OpLeftJoin) {
			
			OpLeftJoin oplj = (OpLeftJoin) query;
			return new Binding(createScopeBlocks(oplj.getLeft()), createScopeBlocks(oplj.getRight()),true);


			
		} else if (query instanceof OpUnion) {
			OpUnion opUnion = (OpUnion) query;
			Set<Binding> bindings = new HashSet<Binding>();
		
			bindings.add(createScopeBlocks(opUnion.getLeft()));
			bindings.add(createScopeBlocks(opUnion.getRight()));

			return new Binding(bindings);
			



		} else if (query instanceof OpBGP) {
			
			
			return new Binding(mconf, new HashSet<Triple>(((OpBGP)query).getPattern().getList()));

		} else
		{

			throw new ImplementationException(
					"Check mappingfinder for processinf of child op:"
							+ query.getName());
		}

	}

	private Map<Op, ScopeBlock> op2ScopeBlock = new HashMap<Op, ScopeBlock>();

	@Override
	public String toString() {

		String str = "MappingFilterFinder for the Query: \n";
		str += "*************************************\n";
		str += query.toString()+"\n";

		
		str += "********************************\n";
		str+= "ScopeBlocks are:\n";
		str += op2ScopeBlock.get(query).toString();
		
		

		return str;
	}

//	public Set<Expr> getFilterForVariables(Op op, Var... var) {
//
//		return op2ScopeBlock.get(op).getFilterFor(var);
//	}
//	
//	
//	public ScopeBlock getScopeBlock(Op op){
//		return op2ScopeBlock.get(op);
//	}
	
	
	
	
	public OpSlice getSlice() {
		return slice;
	}
	public OpOrder getOrder() {
		return order;
	}
	
	public OpProject getProject() {
		return project;
	}
	
	public OpDistinct getDistinct() {
		return distinct;
	}
	
	public OpReduced getReduced(){
		return reduced;
	}

	public void setProject(OpProject opproj) {
		this.project = opproj;
		
	}

}
