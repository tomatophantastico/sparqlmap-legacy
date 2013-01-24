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
import org.openjena.atlas.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.Triple.Field;
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
public class MappingBinding_ {

	private Map<Triple, Collection<TripleMap>> binding = new HashedMap<Triple, Collection<TripleMap>>();
	//private R2RMLModel mapconf;
	private Multimap<String, TermMap> var2Column = HashMultimap.create();
	private List<Expr> filter = new ArrayList<Expr>();
	MappingBinding left;
	MappingBinding right;
	boolean optional;
	private Set<MappingBinding> unionBindings;

	private Logger log = LoggerFactory.getLogger(MappingBinding.class);

	public MappingBinding(MappingBinding left, MappingBinding right,
			boolean isOptional) {
		this.left = left;
		this.right = right;
		this.optional = isOptional;
	}

	/**
	 * union case
	 * 
	 * @param union
	 *            all binding to be unioned
	 */
	public MappingBinding(Set<MappingBinding> union) {
		this.unionBindings = union;
	}
	
	public MappingBinding(){
		super();
	}

	public MappingBinding(R2RMLModel mapConf, Set<Triple> triples) {
		this.mapconf = mapConf;

		for (Triple triple : triples) {
			addTriple(triple);
		}
	}

	public void addTriple(Triple triple) {

		Collection<TripleMap> candidates = new HashSet<TripleMap>();

		for (TripleMap tripleMap : mapconf.getTripleMaps()) {
			candidates.add(tripleMap.getShallowCopy());
		}
		binding.put(triple, candidates);

	}

	public void joins(MappingBinding sbnm) {
		throw new ImplementationException("joins is not implemented ");
	}

	/**
	 * this method starts the mapping binding process.
	 */
	public MappingBinding mapIt() {
		
		MappingBinding merged = new MappingBinding(left, right, isOptional);

		// first we bind all triples available to
		initialBinding();

		// the merge all bindings
		// while there are quite a lot of ways of organizing the order of
		// binding, we'll just do it in a quick and dirty way.
		while(mergeDown());

		if (log.isDebugEnabled()) {
			log.debug("Binding before merge ");
			log.debug(toString());
		}
		mergeBinding(this.binding, this.binding);

	}

	/**
	 * merges the bindings. performs the join 
	 */
	private void mergeBinding(Map<Triple, Collection<TripleMap>> binding1,
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
					boolean hadMatch = false;
					if (!(t1 == t2)) {
						for (Triple.Field f1 : fields) {
							for (Triple.Field f2 : fields) {
								Node n1 = f1.getField(t1);
								Node n2 = f2.getField(t2);
								Collection<TripleMap> triplemaps1 = binding1
										.get(t1);
								Collection<TripleMap> triplemaps2 = binding2
										.get(t2);
								if (matches(n1, n2)) {
									wasmergedthisrun = mergeTripleMaps(f1, f2,
											triplemaps1, triplemaps2);
									if (wasmergedthisrun) {
										wasmerged = true;
									}
									hadMatch = true; 
								}
							}
						}
					}
					//the triple shares no variables.
					//we add the triple 
				}
			}
		} while (wasmergedthisrun);

		if (wasmerged) {
			mergeDown();
		}

	}

	/**
	 * goes down the tree and merges everhing.
	 * @return true if a binding was modified
	 */
	private boolean mergeDown() {
		boolean mergedSomething = false;
		
		if (right != null) {
			right.mergeBinding(right.binding, binding);
			if (!optional) {
				mergeBinding(binding, right.binding);
			}
			mergedSomething = right.mergeDown();

		}

		if (left != null) {
			left.mergeBinding(left.binding, binding);
			mergeBinding(binding, left.binding);
		}

		if (unionBindings != null) {
			for (MappingBinding ubinding : unionBindings) {
				ubinding.mergeBinding(ubinding.binding, binding);
			}
		}
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
			for (PO po1 : new HashSet<PO>(triplemap1.getPos())) {
				Set<PO> toRetain = new HashSet<TripleMap.PO>();

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
				mergedSomething = triplemap1.getPos().retainAll(toRetain);
			}
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

	/**
	 * Creates an initial set of bindings according to the triples. Also calls
	 * the initial binding on all child mapping bindings
	 */
	private void initialBinding() {
		// going down

		if (left != null) {
			left.initialBinding();
		}
		if (right != null) {
			right.initialBinding();
		}

		if (unionBindings != null) {
			for (MappingBinding union : this.unionBindings) {
				union.initialBinding();
			}
		}
		// do the actual initialization.
		bindTripleMapsToTriples();

	}

	private void bindTripleMapsToTriples() {
		for (final Triple triple : binding.keySet()) {
			log.debug("Initial Binding for triple :" + triple);
			Node s = getEquals((Var) triple.getSubject());
			Node p = getEquals((Var) triple.getPredicate());
			Node o = getEquals((Var) triple.getObject());
			Collection<TripleMap> trms = binding.get(triple);

			// iterate over the subjects and remove them if they are not
			// compatible
			for (TripleMap tripleMap : new HashSet<TripleMap>(trms)) {
				if (!tripleMap.getSubject().getCompChecker().isCompatible(s)) {
					trms.remove(tripleMap);
					if (log.isDebugEnabled()) {
						log.debug("Removing triple map because of subject compatibility:"
								+ tripleMap);
					}
				} else {
					// we can now check for PO
					for (PO po : new HashSet<PO>(tripleMap.getPos())) {
						if (!po.getPredicate().getCompChecker().isCompatible(p)) {
							tripleMap.getPos().remove(po);
							if (log.isDebugEnabled()) {
								log.debug("Removing PO  because of predicate compatibility:"
										+ tripleMap.getSubject() + " " + po);
							}
						} else if (!po.getObject().getCompChecker()
								.isCompatible(o)) {
							tripleMap.getPos().remove(po);
							if (log.isDebugEnabled()) {
								log.debug("Removing PO because of object compatibility:"
										+ tripleMap.getSubject() + " " + po);
							}
						}
					}
					if (tripleMap.getPos().isEmpty()) {
						trms.remove(tripleMap);
						if (log.isDebugEnabled()) {
							log.debug("Removing triple map POs are empty:"
									+ tripleMap);
						}
					}
				}
			}
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

	public Map<Triple, Collection<TripleMap>> getBinding() {
		return binding;
	}

	public void addFilter(List<Expr> exprs) {
		filter.addAll(exprs);
	}

	public Node getEquals(Var var) {
		Set<Node> eqNodes = new HashSet<Node>();

		for (Expr expr : filter) {
			if (expr instanceof E_Equals) {
				E_Equals ee = (E_Equals) expr;
				ExprVar eqVar = ee.getArg1().getExprVar();
				NodeValue eqTerm = ee.getArg2().getConstant();
				// try other way around;
				if (eqVar == null && eqTerm == null) {
					eqVar = ee.getArg2().getExprVar();
					eqTerm = ee.getArg1().getConstant();
				}
				if (eqVar != null
						&& eqVar.getVarName().equals(var.getVarName())
						&& eqTerm != null && eqTerm instanceof NodeValue) {
					eqNodes.add(((NodeValue) eqTerm).getNode());
				}
			}
		}

		if (eqNodes.isEmpty()) {
			return var.asNode();
		} else if (eqNodes.size() == 1) {
			return eqNodes.iterator().next();
		} else {
			throw new ImplementationException(
					"Implement behavior for multiple equals on a single varaible");
		}
	}

	public String getResource(Var var) {
		Set<String> eqNodes = new HashSet<String>();

		for (Expr expr : filter) {
			if (expr instanceof E_Equals) {
				E_Equals ee = (E_Equals) expr;
				ExprVar eqVar = ee.getArg1().getExprVar();
				NodeValue eqTerm = ee.getArg2().getConstant();
				// try other way around;
				if (eqVar == null && eqTerm == null) {
					eqVar = ee.getArg2().getExprVar();
					eqTerm = ee.getArg1().getConstant();
				}

				if (eqVar != null
						&& eqVar.getVarName().equals(var.getVarName())
						&& eqTerm != null && eqTerm instanceof NodeValueNode) {
					eqNodes.add(((NodeValueNode) eqTerm).getNode().getURI());
				}
			}
		}

		if (eqNodes.isEmpty()) {
			return null;
		} else if (eqNodes.size() == 1) {
			return eqNodes.iterator().next();
		} else {
			throw new ImplementationException(
					"Implement behavior for multiple equals on a single varaible");
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Triple Bindings are: \n");
		Set<Triple> triples = this.binding.keySet();
		for (Triple triple : triples) {
			sb.append("* " + triple.toString() + "\n");
			for (TripleMap tm : this.binding.get(triple)) {
				sb.append("    Triplemap: " + tm + "\n");
				for (PO po : tm.getPos()) {
					sb.append("     PO:" + po.getPredicate().toString() + " "
							+ po.getObject().toString() + "\n");
				}
			}
		}
		return sb.toString();
	}
}