package org.aksw.sparqlmap.core.mapper.finder;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.aksw.sparqlmap.core.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap.PO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.Triple.Field;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorByTypeBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.Table;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.algebra.table.TableUnit;
import com.hp.hpl.jena.sparql.expr.Expr;


/**
 * this class generates a MappingBinding for a query by walking all over it.
 * @author joerg
 *
 */
public class Binder {
	private static Logger log = LoggerFactory.getLogger(Binder.class);
	
	private QueryInformation qi;
	private R2RMLModel mapconf;

	
	public Binder(R2RMLModel mappingConf, QueryInformation qi) {
		this.mapconf = mappingConf;
		this.qi = qi;
	}


	public MappingBinding bind(Op op){
		
		Map<Triple, Collection<TripleMap>> bindingMap = new HashMap<Triple, Collection<TripleMap>>();
		
		OpWalker.walk(op, new BinderVisitor(qi.getFiltersforvariables(), bindingMap));
		
		

		return new MappingBinding(bindingMap);
	}
	
	
	
	private class BinderVisitor extends OpVisitorByTypeBase{
		Map<Triple, Map<String,Collection<Expr>>> triples2variables2expressions;
		Map<Triple, Collection<TripleMap>> binding;
		
		
		
		
		public BinderVisitor(
				Map<Triple, Map<String, Collection<Expr>>> triples2variables2expressions,
				Map<Triple, Collection<TripleMap>> binding) {
			super();
			this.triples2variables2expressions = triples2variables2expressions;
			this.binding = binding;
		}


		// we use this stack to track track which what to merge on unions, joins and left joins
		Stack<Collection<Triple>> triples = new Stack<Collection<Triple>>();
		
		
		@Override
		public void visit(OpJoin opJoin) {
			log.debug("Visiting opJoin " + opJoin);
			Collection<Triple> rightSideTriples = triples.pop();

			Collection<Triple> leftSideTriples = triples.pop();
			
			
			//we now merge the bindings for each and every triple we got here.
			
			boolean changed = mergeBinding(partitionBindings(leftSideTriples), partitionBindings(rightSideTriples));
			changed = changed || mergeBinding(partitionBindings(rightSideTriples), partitionBindings(leftSideTriples));

			//if we modified any binding, we have to walk this part of the Op-Tree again.
			
			if(changed){
				OpWalker.walk(opJoin, this);

			}
			mergeAndPutOnStack(leftSideTriples, rightSideTriples);
		}

		@Override
		public void visit(OpLeftJoin opLeftJoin) {
			log.debug("Visiting opLeftJoin"+opLeftJoin);
			
			if(opLeftJoin.getLeft() instanceof OpTable && ((OpTable)opLeftJoin.getLeft()).getTable() instanceof TableUnit){
				//leftjoin without triples. do nothing
				
			}else{
				Collection<Triple> rightSideTriples = triples.pop();
				Collection<Triple> leftSideTriples = triples.pop();
				//we now merge the bindings for each and every triple we got here.
				
				boolean changed =  mergeBinding(partitionBindings(rightSideTriples), partitionBindings(leftSideTriples));
	
				//if we modified any binding, we have to walk this part of the Op-Tree again.
				
				if(changed){
					OpWalker.walk(opLeftJoin, this);
				}
				mergeAndPutOnStack(leftSideTriples, rightSideTriples);
			}
		}
		
		
		
		@Override
		public void visit(OpUnion opUnion) {
			log.debug("Visiting opUnion" + opUnion);
			//just popping the triples, so they are not used later on 
			Collection<Triple> rightSideTriples = triples.pop();
			Collection<Triple> leftSideTriples = triples.pop();
			
			mergeAndPutOnStack(leftSideTriples, rightSideTriples);
			
			
		}

		private void mergeAndPutOnStack(Collection<Triple> leftSideTriples,
				Collection<Triple> rightSideTriples) {
			//do not nothing to the triples but put them together, so they can be merged by a later join
			Collection<Triple> combined = new HashSet<Triple>();
			combined.addAll(leftSideTriples);
			combined.addAll(rightSideTriples);
			triples.add(combined);
		}
		
		
		
		
		@Override
		public void visit(OpBGP opBGP) {
			triples.add(opBGP.getPattern().getList());
			
			for(Triple triple: opBGP.getPattern().getList()){
				if(!binding.containsKey(triple)){
					initialBinding(triple);
				}
			}
			
			// now merge them
			boolean hasMerged = false;
			do{
				hasMerged = mergeBinding(partitionBindings(opBGP.getPattern().getList()), partitionBindings(opBGP.getPattern().getList()));
			}while(hasMerged);
			
		}
		
		
		/**creates a subset of the bindings
		 * 
		 * @return
		 */
		private Map<Triple,Collection<TripleMap>> partitionBindings(Collection<Triple> triples){
			Map<Triple,Collection<TripleMap>> subset = new HashMap<Triple, Collection<TripleMap>>();
			for(Triple triple : triples){
				subset.put(triple, binding.get(triple));
			}
			
			return subset;
		}


		private void initialBinding(Triple triple) {
			
			// first bind all triple maps to the triple
			Collection<TripleMap> trms = new HashSet<TripleMap>();

			for (TripleMap tripleMap : mapconf.getTripleMaps()) {
				trms.add(tripleMap.getShallowCopy());
			}
			binding.put(triple, trms);
			
		
			//then check them for compatibility
			Map<String,Collection<Expr>> var2exps = triples2variables2expressions.get(triple);
			String sname = triple.getSubject().getName();
			String pname =  triple.getPredicate().getName();
			String oname =  triple.getObject().getName();
			Collection<Expr> sxprs =  var2exps.get(sname);
			Collection<Expr> pxprs = var2exps.get(pname);
			Collection<Expr> oxprs = var2exps.get(oname);
		
			// iterate over the subjects and remove them if they are not
			// compatible
			for (TripleMap tripleMap : new HashSet<TripleMap>(trms)) {
				if (!tripleMap.getSubject().getCompChecker().isCompatible(sname,sxprs)) {
					trms.remove(tripleMap);
//					if (log.isDebugEnabled()) {
//						log.debug("Removing triple map because of subject compatibility:"
//								+ tripleMap);
//					}
				} else {
					// we can now check for PO
					for (PO po : new HashSet<PO>(tripleMap.getPos())) {
						if (!po.getPredicate().getCompChecker().isCompatible(pname,pxprs)) {
							tripleMap.getPos().remove(po);
//							if (log.isDebugEnabled()) {
//								log.debug("Removing PO  because of predicate compatibility:"
//										+ tripleMap.getSubject() + " " + po);
//							}
						} else if (!po.getObject().getCompChecker()
								.isCompatible(oname,oxprs)) {
							tripleMap.getPos().remove(po);
//							if (log.isDebugEnabled()) {
//								log.debug("Removing PO because of object compatibility:"
//										+ tripleMap.getSubject() + " " + po);
//							}
						}
					}
					if (tripleMap.getPos().isEmpty()) {
						trms.remove(tripleMap);
//						if (log.isDebugEnabled()) {
//							log.debug("Removing triple map POs are empty:"
//									+ tripleMap);
						
					}
				}
				
			}
			if(log.isDebugEnabled()){
				log.debug("Initial binding for triple " +triple   );
				log.debug("" + binding.get(triple));
			}
		

		}
		
		
		
		
	}
	
	
	/**
	 * merges the bindings. performs the join 
	 */
	private boolean mergeBinding(Map<Triple, Collection<TripleMap>> binding1,
			Map<Triple, Collection<TripleMap>> binding2) {
		Triple.Field[] fields = { Triple.Field.fieldSubject,
				Triple.Field.fieldPredicate, Triple.Field.fieldObject };

		boolean wasmerged = false;

		// <PO> toBeRemoved = new ArrayList<TripleMap.PO>();
		boolean wasmergedthisrun = false;
		do {
			wasmergedthisrun = false;
			for (Triple t1 : new HashSet<Triple>(binding1.keySet())) {
				for (Triple t2 : binding2.keySet()) {
					if (!(t1 == t2)) {
						for (Triple.Field f1 : fields) {
							for (Triple.Field f2 : fields) {
					
								Node n1 = f1.getField(t1);
								Node n2 = f2.getField(t2);
								Collection<TripleMap> triplemaps1 = binding1
										.get(t1);
								Collection<TripleMap> triplemaps1_copy= null;
								if(log.isDebugEnabled()){
									triplemaps1_copy = new HashSet<TripleMap>(binding1
											.get(t1));
								}
										
								
								Collection<TripleMap> triplemaps2 = binding2
										.get(t2);
								if (matches(n1, n2)) {
									wasmergedthisrun = mergeTripleMaps(f1, f2,
											triplemaps1, triplemaps2);
									if (wasmergedthisrun) {
										wasmerged = true;
									}
									if(log.isDebugEnabled()){
										if(wasmergedthisrun){
											log.debug("Merged on t1: " + t1.toString() + " x t2:" + t2.toString());
											log.debug("Removed the following triple maps:");
											
											triplemaps1_copy.removeAll(triplemaps1);
											for (TripleMap tripleMap : triplemaps1_copy) {
												log.debug("" +  tripleMap);
											}
										}else{
											log.debug("All compatible on t1: " + t1.toString() + " x t2:" + t2.toString());

										}
										
									}
									
									
								}
							}
						}
					}
					//the triple shares no variables.
					//we add the triple 
				}
			}
		} while (wasmergedthisrun);

		return wasmerged;

	}
	
	
	/**
	 * modifies n1 according to doing a join on with n2
	 * 
	 * @return true if something was modified
	 * @param n1
	 * @param n2
	 * @param f1
	 * @param f2
	 * @param triplemaps1
	 * @param triplemaps2
	 */
	private boolean mergeTripleMaps(Field f1, Field f2,
			Collection<TripleMap> triplemaps1, Collection<TripleMap> triplemaps2) {
		// we keep track if a modification was performed. Needed later to notify
		// the siblings.
		boolean mergedSomething = false;

		// we iterate over all triplemaps of both (join-style)
		for (TripleMap triplemap1 : new HashSet<TripleMap>(triplemaps1)) {
			Set<PO> toRetain = new HashSet<TripleMap.PO>();
			for (PO po1 : new HashSet<PO>(triplemap1.getPos())) {
				for (TripleMap triplemap2 : triplemaps2) {
					// we iterate over the PO, as each generates a triple per
					// row.
					for (PO po2 : triplemap2.getPos()) {
						TermMap tm1 = getTermMap(po1, f1);
						TermMap tm2 = getTermMap(po2, f2);
						if (tm1.getCompChecker().isCompatible(tm2)) {
							// they are compatible! we keep!
							toRetain.add(po1);

						}
					}
				}
			}
			mergedSomething = triplemap1.getPos().retainAll(toRetain);

			if (triplemap1.getPos().size() == 0) {
				triplemaps1.remove(triplemap1);
			}
		}
		return mergedSomething;

	}
	
	private TermMap getTermMap(PO po, Field field) {
		TermMap result = null;
		if (field == Triple.Field.fieldSubject) {
			result = po.getTripleMap().getSubject();
		}
		if (field == Triple.Field.fieldPredicate) {
			result = po.getPredicate();
		}
		if (field == Triple.Field.fieldObject) {
			result = po.getObject();

		}

		return result;
	}
	
	/**
	 * checks if both are variables with the same name
	 * 
	 * @param n1
	 * @param n2
	 * @return
	 */
	private boolean matches(Node n1, Node n2) {
		boolean result = false;
		if (n1.isVariable() && n2.isVariable()
				&& n1.getName().equals(n2.getName())) {
			result = true;
		}
		return result;
	}

	
	


}
