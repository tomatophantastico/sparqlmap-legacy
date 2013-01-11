package org.aksw.sparqlmap.mapper.finder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.mapper.translate.ImplementationException;
import org.apache.commons.collections15.map.HashedMap;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.query.Mapping;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;

/**
 * this class contains all the information to what mappings a node will map.
 * 
 * @author joerg
 * 
 */
public class MappingBinding {

	private Multimap<Triple, TripleMap> binding = HashMultimap.create();
	private R2RMLModel mapconf;
	private Multimap<String, TermMap> var2Column = HashMultimap.create();
	private List<Expr> filter = new ArrayList<Expr>();
	MappingBinding left;
	MappingBinding right;
	private Set<MappingBinding> union;


	public MappingBinding(MappingBinding left, MappingBinding right,
			boolean isOptional) {
		throw new ImplementationException("not implemented");
	}
	
	/**
	 * union case
	 * 
	 * @param union
	 * all binding to be unioned
	 */
	public MappingBinding(Set<MappingBinding> union) {
		throw new ImplementationException("not implemented");
	}
	
	public MappingBinding(R2RMLModel mapConf, Set<Triple> triples) {
		this.mapconf = mapConf;

		for (Triple triple : triples) {
			addTriple(triple);
		}
	}

	
	
	
	

	public void addTriple(Triple triple) {

		Set<TripleMap> candidates = mapconf.getTripleMaps();

		for (TripleMap tripleMap : candidates) {
			binding.put(triple, tripleMap.getShallowCopy());
		}

	}

	public void joins(MappingBinding sbnm) {
		throw new ImplementationException("joins is not implemented ");
	}

	public void mapIt() {
		
		if(mapconf !=null){
			initialBinding();
		} else if(left!=null&&right!=null){
			throw new ImplementationException("Implement me");
		} else if (union!=null){
			throw new ImplementationException("Implement me");
			
		}else{
			throw new ImplementationException("should never come here");
		}

	}

	private void initialBinding() {
		Map<Triple,Collection<TripleMap>> skeepersMap = new HashedMap<Triple, Collection<TripleMap>>();
		Map<Triple,Collection<TripleMap>> pokeepersMap = new HashedMap<Triple, Collection<TripleMap>>();

		for (final Triple triple : binding.keySet()) {
			
			
			// remove for s
			Node s = getEquals((Var) triple.getSubject());
			Collection<TripleMap> trms = binding.get(triple);
			Collection<TripleMap> skeepers = new HashSet<TripleMap>();
			
			for (TripleMap tripleMap : trms) {
				if(tripleMap.getSubject().getCompChecker().isCompatible(s)){
					skeepers.add(tripleMap);
				}
			}
			
			skeepersMap.put(triple, skeepers);
			
			
			// now for P and O
			Collection<TripleMap> pokeepers = new HashSet<TripleMap>();
			Node p = getEquals((Var) triple.getPredicate());
			Node o = getEquals((Var) triple.getObject());
			
			for (TripleMap tripleMap : trms) {
				Collection<PO> pos = tripleMap.getPos();
				Collection<PO> posKeepers = new HashSet<TripleMap.PO>();
				for (PO po : pos) {
					if(po.getPredicate()
							.getCompChecker().isCompatible(p) && 
							po.getObject().getCompChecker().isCompatible(o)){
						posKeepers.add(po);
					}else{
						
						//just here to set a breakpoint
						po.getPredicate();
					}
				}
				if(posKeepers.size()>0){
					tripleMap.getPos().retainAll(posKeepers);
					pokeepers.add(tripleMap);
				}
	
			}
			pokeepersMap.put(triple, pokeepers);
			
		}
		//now actually remove them
		for (Triple striple : skeepersMap.keySet()) {
			binding.get(striple).retainAll(skeepersMap.get(striple));
		}
		for (Triple striple : pokeepersMap.keySet()) {
			binding.get(striple).retainAll(pokeepersMap.get(striple));
		}
	}

	
	protected Collection<TermMap> getColumn(String var) {
		return Collections.unmodifiableCollection(var2Column.get(var));
	}

	public Collection<TermMap> getColumn(String var, Mapping mapping) {
		Set<TermMap> cols = new HashSet<TermMap>();
		for (TermMap col : getColumn(var)) {
			if (col.getTripleMap().equals(mapping)) {
				cols.add(col);
			}
		}
		return cols;
	}

	public Multimap<Triple, TripleMap> getBinding() {
		return binding;
	}

	public void addFilter(List<Expr> exprs) {
		filter.addAll(exprs);
	}
	
	public Node getEquals(Var var) {
		Set<Node> eqNodes= new HashSet<Node>(); 
		
		for (Expr expr : filter) {
			if (expr instanceof E_Equals) {
				E_Equals ee = (E_Equals) expr;
					ExprVar eqVar  = ee.getArg1().getExprVar();
					NodeValue eqTerm = ee.getArg2().getConstant();
					//try other way around;
					if(eqVar == null&& eqTerm == null){
						eqVar = ee.getArg2().getExprVar();
						eqTerm = ee.getArg1().getConstant();
					}
					if(eqVar != null && eqVar.getVarName().equals(var.getVarName()) && eqTerm != null && eqTerm instanceof NodeValue){
						eqNodes.add(((NodeValue)eqTerm).getNode()); 
					}
				}
		}

		if(eqNodes.isEmpty()){
			return var.asNode();
		}else if(eqNodes.size()==1){
			return eqNodes.iterator().next();
		}else{
			throw new ImplementationException("Implement behavior for multiple equals on a single varaible");
		}
	}
	
	
	public String getResource(Var var){
Set<String> eqNodes= new HashSet<String>(); 
		
		for (Expr expr : filter) {
			if (expr instanceof E_Equals) {
				E_Equals ee = (E_Equals) expr;
					ExprVar eqVar  = ee.getArg1().getExprVar();
					NodeValue eqTerm = ee.getArg2().getConstant();
					//try other way around;
					if(eqVar == null&& eqTerm == null){
						eqVar = ee.getArg2().getExprVar();
						eqTerm = ee.getArg1().getConstant();
					}
					
					if(eqVar != null && eqVar.getVarName().equals(var.getVarName()) && eqTerm != null && eqTerm instanceof NodeValueNode){
						eqNodes.add(((NodeValueNode)eqTerm).getNode().getURI()); 
					}
				}
		}

		if(eqNodes.isEmpty()){
			return null;
		}else if(eqNodes.size()==1){
			return eqNodes.iterator().next();
		}else{
			throw new ImplementationException("Implement behavior for multiple equals on a single varaible");
		}
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Triple Bindings are: \n");
		Set<Triple> triples = this.binding.keySet();
		for(Triple triple : triples){
			sb.append("* " + triple.toString() + "\n");
			for(TripleMap tm : this.binding.get(triple)){
				sb.append("    Triplemap: " + tm.getSubject().toString() + "\n");
				for(PO po: tm.getPos()){
					sb.append("     PO:" + po.getPredicate().toString() + " " +  po.getObject().toString()+ "\n");
				}
			}
		}
		return sb.toString();
	}
}