package org.aksw.sparqlmap.core.mapper.finder;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.core.mapper.translate.QuadVisitorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorByTypeBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.Table;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadBlock;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.algebra.table.TableUnit;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.QuadPattern;
import com.hp.hpl.jena.sparql.expr.Expr;


/**
 * this class generates a MappingBinding for a query by walking all over it.
 * @author joerg
 *
 */
public class Binder {
	private static Logger log = LoggerFactory.getLogger(Binder.class);
	
	private TranslationContext tc;
	private R2RMLModel mapconf;

	
	public Binder(R2RMLModel mappingConf, TranslationContext tc) {
		this.mapconf = mappingConf;
		this.tc = tc;
	}


	public MappingBinding bind(Op op){
		
		Map<Quad, Collection<TripleMap>> bindingMap = new HashMap<Quad, Collection<TripleMap>>();
		
		OpWalker.walk(op, new BinderVisitor(tc.getQueryInformation().getFiltersforvariables(), bindingMap));
		
		

		return new MappingBinding(bindingMap);
	}
	
	
	
	private class BinderVisitor extends QuadVisitorBase{
		
		
		Map<Quad, Map<String,Collection<Expr>>> quads2variables2expressions;
		Map<Quad, Collection<TripleMap>> binding;
		
		
		
		
		public BinderVisitor(
				Map<Quad, Map<String, Collection<Expr>>> quads2variables2expressions,
				Map<Quad, Collection<TripleMap>> binding) {
			super();
			this.quads2variables2expressions = quads2variables2expressions;
			this.binding = binding;
		}


		// we use this stack to track track which what to merge on unions, joins and left joins
		Stack<Collection<Quad>> quads = new Stack<Collection<Quad>>();
		
		
		@Override
		public void visit(OpJoin opJoin) {
			log.debug("Visiting opJoin " + opJoin);
			Collection<Quad> rightSideTriples = quads.pop();

			Collection<Quad> leftSideTriples = quads.pop();
			
			
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
				Collection<Quad> rightSideTriples = quads.pop();
				Collection<Quad> leftSideTriples = quads.pop();
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
			Collection<Quad> rightSideTriples = quads.pop();
			Collection<Quad> leftSideTriples = quads.pop();
			
			mergeAndPutOnStack(leftSideTriples, rightSideTriples);
			
			
		}

		private void mergeAndPutOnStack(Collection<Quad> leftSideTriples,
				Collection<Quad> rightSideTriples) {
			//do not nothing to the triples but put them together, so they can be merged by a later join
			Collection<Quad> combined = new HashSet<Quad>();
			combined.addAll(leftSideTriples);
			combined.addAll(rightSideTriples);
			quads.add(combined);
		}
		
		
		
		
		@Override
		public void visit(OpQuadPattern opQuadBlock) {
			quads.add(opQuadBlock.getPattern().getList());
			
			for(Quad quad: opQuadBlock.getPattern().getList()){
				if(!binding.containsKey(quad)){
					initialBinding(quad);
				}
			}
			
			// now merge them
			boolean hasMerged = false;
			do{
				hasMerged = mergeBinding(partitionBindings(opQuadBlock.getPattern().getList()), partitionBindings(opQuadBlock.getPattern().getList()));
			}while(hasMerged);
			
		}
		
		
		/**creates a subset of the bindings
		 * 
		 * @return
		 */
		private Map<Quad,Collection<TripleMap>> partitionBindings(Collection<Quad> quads){
			Map<Quad,Collection<TripleMap>> subset = new HashMap<Quad, Collection<TripleMap>>();
			for(Quad quad : quads){
				subset.put(quad, binding.get(quad));
			}
			
			return subset;
		}


		private void initialBinding(Quad quad) {
			
			// first bind all triple maps to the triple
			Collection<TripleMap> trms = new HashSet<TripleMap>();

			for (TripleMap tripleMap : mapconf.getTripleMaps()) {
				trms.add(tripleMap.getShallowCopy());
			}
			binding.put(quad, trms);
			
		
			//then check them for compatibility
			Map<String,Collection<Expr>> var2exps = quads2variables2expressions.get(quad);
			String gname = quad.getGraph().getName();
			String sname = quad.getSubject().getName();
			String pname =  quad.getPredicate().getName();
			String oname =  quad.getObject().getName();
			Collection<Expr> sxprs =  var2exps.get(sname);
			Collection<Expr> pxprs = var2exps.get(pname);
			Collection<Expr> oxprs = var2exps.get(oname);
			Collection<Expr> gxprs = var2exps.get(gname);
		
			// iterate over the subjects and remove them if they are not
			// compatible
			for (TripleMap tripleMap : new HashSet<TripleMap>(trms)) {
				
				if(!tripleMap.getGraph().getCompChecker().isCompatible(gname,gxprs)){
					trms.remove(tripleMap);
					
				}else if (!tripleMap.getSubject().getCompChecker().isCompatible(sname,sxprs)) {
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
				log.debug("Initial binding for triple " +quad   );
				log.debug("" + binding.get(quad));
			}
		

		}
		
		
		
		
	}
	
	
	enum Field {graph,subject,predicate,object};
	
	private Node getField(Quad quad, Field field){
		switch (field) {
		case graph:
			return quad.getGraph();
		case subject:
			return quad.getSubject();
		case predicate:
			return quad.getPredicate();
		case object:
			return quad.getObject();
		default:
			return null;
		}
		
	}
	
	
	/**
	 * merges the bindings. performs the join 
	 */
	private boolean mergeBinding(Map<Quad, Collection<TripleMap>> binding1,
			Map<Quad, Collection<TripleMap>> binding2) {
		

		boolean wasmerged = false;

		// <PO> toBeRemoved = new ArrayList<TripleMap.PO>();
		boolean wasmergedthisrun = false;
		do {
			wasmergedthisrun = false;
			for (Quad quad1 : new HashSet<Quad>(binding1.keySet())) {
				for (Quad quad2 : binding2.keySet()) {
					if (!(quad1 == quad2)) {
						for (Field f1 : Field.values()) {
							for (Field f2 : Field.values()) {
					
								Node n1 = getField(quad1, f1);
								Node n2 = getField(quad2, f2);
								Collection<TripleMap> triplemaps1 = binding1
										.get(quad1);
								Collection<TripleMap> triplemaps1_copy= null;
								if(log.isDebugEnabled()){
									triplemaps1_copy = new HashSet<TripleMap>(binding1
											.get(quad1));
								}
										
								
								Collection<TripleMap> triplemaps2 = binding2
										.get(quad2);
								if (matches(n1, n2)) {
									wasmergedthisrun = mergeTripleMaps(f1, f2,
											triplemaps1, triplemaps2);
									if (wasmergedthisrun) {
										wasmerged = true;
									}
									if(log.isDebugEnabled()){
										if(wasmergedthisrun){
											log.debug("Merged on t1: " + quad1.toString() + " x t2:" + quad2.toString());
											log.debug("Removed the following triple maps:");
											
											triplemaps1_copy.removeAll(triplemaps1);
											for (TripleMap tripleMap : triplemaps1_copy) {
												log.debug("" +  tripleMap);
											}
										}else{
											log.debug("All compatible on t1: " + quad1.toString() + " x t2:" + quad2.toString());

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
		if (field == Field.subject) {
			result = po.getTripleMap().getSubject();
		} else	if (field == Field.predicate) {
			result = po.getPredicate();
		} else 	if (field == Field.object) {
			result = po.getObject();
		}else if(field == Field.graph){
			result = po.getTripleMap().getGraph();
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
