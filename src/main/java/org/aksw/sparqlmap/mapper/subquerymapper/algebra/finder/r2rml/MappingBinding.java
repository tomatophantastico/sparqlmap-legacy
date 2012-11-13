package org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

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

	public MappingBinding(MappingBinding left, MappingBinding right,
			boolean isOptional) {

	}
	
	private List<Expr> filter = new ArrayList<Expr>();

	private boolean isOptional;
	MappingBinding left;
	MappingBinding right;

	/**
	 * union case
	 * 
	 * @param union
	 *            all binding to be unioned
	 */
	public MappingBinding(Set<MappingBinding> union) {

	}

	private Set<MappingBinding> union;

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

	}

	public void mapIt() {
		
		if(mapconf !=null){
			initialBinding();
		} else if(left!=null&&right!=null){
			
		} else if (union!=null){
			
		}else{
			throw new ImplementationException("should never come here");
		}

	}

	private void initialBinding() {
		

		// now for P and O
		for (final Triple triple : binding.keySet()) {
			
			
			
			Node s = getEquals((Var) triple.getSubject());
			// remove for s
			Collection<TripleMap> trms = binding.get(triple);
			Collection<TripleMap> keepers = new HashSet<TripleMap>();
			
			for (TripleMap tripleMap : trms) {
				if(tripleMap.getSubject().getCompChecker().isCompatible(s)){
					keepers.add(tripleMap);
				}
			}
			
			
			trms.retainAll(keepers);
			keepers = new HashSet<TripleMap>();
			Node p =getEquals((Var) triple.getPredicate());
			Node o = getEquals((Var) triple.getObject());
			
			for (TripleMap tripleMap : trms) {
				Collection<PO> pos = tripleMap.getPos();
				Collection<PO> posKeepers = new HashSet<TripleMap.PO>();
				for (PO po : pos) {
					if(po.getPredicate()
							.getCompChecker().isCompatible(p) && po.getObject()
							.getCompChecker().isCompatible(o)){
						posKeepers.add(po);
					}else{
						
						//just here to set a breakpoint
						po.getPredicate();
					}
				}
				if(posKeepers.size()>0){
					tripleMap.getPos().retainAll(posKeepers);
					keepers.add(tripleMap);
				}
	
			}
			
			trms.retainAll(keepers);
			
			
		
			
			
			
			
			
//
//			trms = Collections2.transform(trms, new Function<TripleMap, TripleMap>() {
//				@Override
//				public TripleMap apply(TripleMap input) {
//
//					Collection<PO> pos = input.getPos();
//					Collections2.filter(pos, new Predicate<PO>() {
//
//						@Override
//						public boolean apply(PO input) {
//							boolean presult = ;
//							boolean oresult = input.getObject()
//									.getCompChecker().isCompatible(o);
//
//							return presult && oresult;
//						}
//					});
//
//					if (input.getPos().size() > 0) {
//						return input;
//					} else {
//						return null;
//					}
//
//				}
//			});
//
		}
	}

	private R2RMLModel mapconf;

	
	// private Map<String, Set<Mapping>> ldp2mappings = new HashMap<String,
	// Set<Mapping>>();
	private Multimap<String, TermMap> var2Column = HashMultimap.create();

	// public Map<String, Set<Mapping>> getLdp2mappings() {
	// return Collections.unmodifiableMap(ldp2mappings);
	// }

	protected Collection<TermMap> getColumn(String var) {
		return Collections.unmodifiableCollection(var2Column.get(var));
	}

	public Collection<TermMap> getColumn(String var, Mapping map) {
		Set<TermMap> cols = new HashSet<TermMap>();
		for (TermMap col : getColumn(var)) {
			if (col.getTripleMap().equals(map)) {
				cols.add(col);
			}
		}

		return cols;
	}

//	@Override
//	public String toString() {
//		return toString(0);
//	}
//
//	public String toString(int indent) {
//		StringBuffer sb = new StringBuffer();
//		String pre = "\n";
//		for (int i = 0; i < indent; i++) {
//			pre += "++";
//		}
//
//		sb.append(pre + "s-block, common s is:" + s.toString() + "");
//
//		for (String var : var2Column.keySet()) {
//			sb.append("\n" + pre + var.toString() + " maps to: ");
//			for (TermMap coldef : var2Column.get(var)) {
//				sb.append(coldef.toString() + ", ");
//			}
//		}
//
//		return sb.toString();
//	}

	// public Set<TermMap> getColdefsFor(Property prop) {
	// Set<TermMap> cols = new HashSet<TermMap>();
	//
	// for (String ldp : ldp2mappings.keySet()) {
	// for (Mapping map : ldp2mappings.get(ldp)) {
	// cols.addAll(map.getTermMap(prop));
	// }
	// }
	// return cols;
	// }

	// /**
	// * removes all mappings and ldps that are not in use by some column
	// */
	// public void cleanUpLdpsMappings() {
	// Set<String> toRetainLdp = null;
	// Set<Mapping> toRetainMap = null;
	// for (Node_Variable var : var2Column.keySet()) {
	// if (var != s) {
	// Set<String> ldpset = new HashSet<String>();
	// Set<Mapping> mapset = new HashSet<Mapping>();
	// Collection<TermMap> coldefs = var2Column.get(var);
	// for (TermMap coldef : coldefs) {
	// mapset.add(coldef.getMapp());
	// ldpset.add(coldef.getMapp().getLinkedDataPath());
	// }
	//
	// if(toRetainLdp == null){
	// toRetainLdp = ldpset;
	// }else{
	// toRetainLdp.retainAll(ldpset);
	// }
	// if(toRetainMap == null){
	// toRetainMap = mapset;
	// }else{
	// toRetainMap.retainAll(mapset);
	// }
	// }
	// }
	//
	// //mappings.retainAll(toRetainMap);
	// if(!toRetainLdp.containsAll(ldps)){
	// retainLdps(toRetainLdp);
	// }
	//
	//
	// }

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
	
//	public Set<Expr>  getFilter(Var... var) {
//		Set<Expr> eqNodes= new HashSet<Expr>(); 
//		
//		for (Expr expr : filter) {
//			if (expr instanceof E_Equals) {
//				E_Equals ee = (E_Equals) expr;
//					ExprVar eqVar  = ee.getArg1().getExprVar();
//					NodeValue eqTerm = ee.getArg2().getConstant();
//					//try other way around;
//					if(eqVar == null&& eqTerm == null){
//						eqVar = ee.getArg2().getExprVar();
//						eqTerm = ee.getArg1().getConstant();
//					}
//					
//					if(eqVar != null && eqVar.getVarName().equals(var.getVarName()) && eqTerm != null && eqTerm instanceof NodeValue){
//						eqNodes.add(((NodeValue)eqTerm).getNode()); 
//						
//					}
//				}
//		}
//		
//		return eqNodes;
//		
//
//	}
	
	
	
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
	
	
	


	
	
	
	
	
	
	
	
	
	

}