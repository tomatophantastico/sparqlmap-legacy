package org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.MappingConfiguration;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.Op0;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.Op2;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpN;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;

public class MappingFilterFinder {

	private MappingConfiguration mconf;
	private Op query;
	
	private OpSlice slice;
	private OpOrder order;
	private OpProject project;
	private OpDistinct distinct;
	private OpReduced reduced;

	public MappingFilterFinder(MappingConfiguration mconf, Op query) {
		super();
		this.mconf = mconf;
		this.query = query;
		createScopeBlocks(query);
	}

	/**
	 * we analyze the query top to bottom and group Ops (especially bgps) into
	 * Scope Blocks. These blocks share the same variable definition there.
	 * 
	 * we further merge all the triples an
	 * 
	 * @param query
	 */
	private void createScopeBlocks(Op query) {

		ScopeBlock block = new ScopeBlock(null,mconf);
		createScopeBlocks(query, block);
		pushDownTheTriples();
		pushDownTheFilters();
		findMappingsForScopeBlocks();
		dothestarlikeanalysis();

	}
	private void pushDownTheTriples() {
		op2ScopeBlock.get(query).pushDownTriples();

	}

	private void pushDownTheFilters() {
		op2ScopeBlock.get(query).pushDownFilters();

	}

	/**
	 * here we check for variables that are used in different positions in
	 * triple patterns we pass this
	 * 
	 */
	private void dothestarlikeanalysis() {
		for(ScopeBlock sb: new HashSet<ScopeBlock>(op2ScopeBlock.values())){
			sb.optimize();
		}
		
		
	}

	private void findMappingsForScopeBlocks() {
		// for each of the scope-blocks we now find the mappings for each triple
		// block.
		
		Set<ScopeBlock> scopeblocks = new HashSet(op2ScopeBlock.values());
		for(ScopeBlock sb : scopeblocks){
			sb.mapToMappingConfiguration();
		}
		
		
		

	}

	private void createScopeBlocks(Op query, ScopeBlock block) {
		op2ScopeBlock.put(query, block);
		block.addOp(query);
		
		
		if(query instanceof OpProject){
			this.project = ((OpProject) query);
		}
		
		if(query instanceof OpReduced){
			this.reduced = ((OpReduced) query);
		}
		
		if (query instanceof OpSlice) {
			OpSlice slice = (OpSlice) query;
			this.slice = slice;
			
		}
		if (query instanceof OpOrder) {
			OpOrder order = (OpOrder) query;
			this.order = order;
			
		}
		
		if(query instanceof OpDistinct){
			this.distinct = (OpDistinct) query;
		}
		

		// check for the children
		// i do not use the OpVisitorBase class here, because the overloaded
		// methods of it do not go well along with polymorphism

		if (query instanceof OpLeftJoin) {
			// the left part of the query contributes to the block
			OpLeftJoin oplj = (OpLeftJoin) query;
			createScopeBlocks(oplj.getLeft(), block);
			createScopeBlocks(oplj.getRight(), new ScopeBlock(block,mconf));

			// the right part not

			;
		} else if (query instanceof OpUnion) {
			OpUnion opUnion = (OpUnion) query;
			createScopeBlocks(opUnion.getLeft(), new ScopeBlock(block,mconf));
			createScopeBlocks(opUnion.getRight(), new ScopeBlock(block,mconf));

		} else if (query instanceof Op1) {
			Op1 op1 = (Op1) query;
			createScopeBlocks(op1.getSubOp(), block);

		} else if (query instanceof Op2) {

			// all other Op2, except for union and left join contribute fully to
			// the scope block.
			Op2 op2 = (Op2) query;
			createScopeBlocks(op2.getLeft(), block);
			createScopeBlocks(op2.getRight(), block);

		} else if (query instanceof OpN) {
			OpN opn = (OpN) query;

			for (Op op : opn.getElements()) {
				createScopeBlocks(op, block);
			}

		} else if (query instanceof Op0) {
			// no need to do anythin, leaf node
			;

		}else
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

	public Set<Expr> getFilterForVariables(Op op, Var... var) {

		return op2ScopeBlock.get(op).getFilterFor(var);
	}
	
	
	public ScopeBlock getScopeBlock(Op op){
		return op2ScopeBlock.get(op);
	}
	
	
	
	
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

}
