package org.aksw.sparqlmap.core.mapper.finder;
//package org.aksw.sparqlmap.mapper.finder;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
//import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
//
//import com.hp.hpl.jena.graph.Node;
//import com.hp.hpl.jena.graph.Triple;
//import com.hp.hpl.jena.sparql.algebra.Op;
//import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
//import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
//import com.hp.hpl.jena.sparql.core.Var;
//import com.hp.hpl.jena.sparql.expr.E_Equals;
//import com.hp.hpl.jena.sparql.expr.Expr;
//import com.hp.hpl.jena.sparql.expr.ExprVar;
//import com.hp.hpl.jena.sparql.expr.NodeValue;
//import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;
//import com.hp.hpl.jena.sparql.util.VarUtils;
//
//public class ScopeBlock {
//
//	static org.slf4j.Logger log = org.slf4j.LoggerFactory
//			.getLogger(ScopeBlock.class);
//
//	private ScopeBlock parent;
//	private Set<ScopeBlock> children = new HashSet<ScopeBlock>();
//	private Set<Triple> triples = new HashSet<Triple>();
//	private Set<Triple> pushedInTriples = new HashSet<Triple>();
//	private Set<Expr> filters = new HashSet<Expr>();
//	private Set<Expr> pushedInFilters = new HashSet<Expr>();
//	private Map<Node, MappingBinding> node2sBlock = new HashMap<Node, MappingBinding>();
//	private Set<Op> ops = new HashSet<Op>();
//	private R2RMLModel mappingConf;
//
//	public ScopeBlock(ScopeBlock parent, R2RMLModel mappingConf) {
//		super();
//		this.mappingConf  = mappingConf;
//		if (parent!=null){
//			this.parent = parent;
//			
//			parent.children.add(this);
//		}
//	}
//
//	public ScopeBlock getParent() {
//		return parent;
//	}
//
//	public Set<ScopeBlock> getChildren() {
//		return children;
//	}
//
//	public void setChildren(Set<ScopeBlock> children) {
//		this.children = children;
//	}
//
//	public void addOp(Op op) {
//		ops.add(op);
//		if (op instanceof OpFilter) {
//			filters.addAll(((OpFilter) op).getExprs().getList());
//		}
//		if (op instanceof OpBGP) {
//			triples.addAll(((OpBGP) op).getPattern().getList());
//		}
//	}
//
//	public Set<Triple> getTriples() {
//		Set<Triple> triples = new HashSet<Triple>();
//		triples.addAll(this.triples);
//		triples.addAll(this.pushedInTriples);
//		return triples;
//	}
//	
//	public void pushDownTriples(){
//		Set<Triple> allTriples = new HashSet<Triple>();
//		allTriples.addAll(pushedInTriples);
//		allTriples.addAll(triples);
//		
//		for(ScopeBlock child: children){
//			child.pushedInTriples.addAll(allTriples);
//		}
//		
//	}
//
//	public void pushDownFilters() {
//		Set<Expr> allFilters = new HashSet<Expr>();
//		allFilters.addAll(filters);
//		allFilters.addAll(pushedInFilters);
//
//		for (Expr filter : allFilters) {
//			for (ScopeBlock child : children) {
//
//				Set<Var> filtervars = filter.getVarsMentioned();
//				Set<Var> childVars = child.getDefinedVars();
//				if (childVars.containsAll(filtervars)) {
//					child.pushedInFilters.add(filter);
//				}
//			}
//		}
//		for (ScopeBlock child : children) {
//			child.pushDownFilters();
//		}
//	}
//
//	/**
//	 * needed only for determining, whether a filter can be pushed down
//	 * 
//	 * @return
//	 */
//	public Set<Var> getDefinedVars() {
//		Set<Var> vars = new HashSet<Var>();
//
//		for (ScopeBlock child : children) {
//			vars.addAll(child.getDefinedVars());
//		}
//
//		for (Expr filter : filters) {
//			vars.addAll(filter.getVarsMentioned());
//		}
//		for (Expr filter : pushedInFilters) {
//			vars.addAll(filter.getVarsMentioned());
//		}
//
//		for (Triple triple : pushedInTriples) {
//			vars.addAll(VarUtils.getVars(triple));
//		}
//		return Collections.unmodifiableSet(vars);
//
//	}
//
//	public Set<Expr> getFilterFor(Var... varArray) {
//		Set<Var> reqVar = new HashSet<Var>(Arrays.asList(varArray));
//		Set<Expr> varFilters = new HashSet<Expr>();
//		Set<Expr> allFilters = new HashSet<Expr>();
//		allFilters.addAll(filters);
//		allFilters.addAll(pushedInFilters);
//
//		for (Expr filterCand : allFilters) {
//			if (reqVar.containsAll(filterCand.getVarsMentioned())) {
//				varFilters.add(filterCand);
//			}
//		}
//
//		return varFilters;
//
//	}
//
//	public void mapToMappingConfiguration() {
//		// we first create the s-blocks
//		Set<Triple> allTriples = new HashSet<Triple>();
//		allTriples.addAll(pushedInTriples);
//		allTriples.addAll(triples);
//
//		// We first separate all triples according to their subjects.
//		Map<Node, Set<Triple>> triplesBySubject = createSBlock(allTriples);
//
//		// we now iterate over for each s-triple block
//
//		for (Node subject : triplesBySubject.keySet()) {
//			Set<Triple> s_triples = triplesBySubject.get(subject);
//			findMappings(subject, s_triples);
//
//		}
//	}
//
//	/**
//	 * 
//	 * @param triples
//	 *            with one subject
//	 */
//	private void findMappings(Node subject, Set<Triple> triples) {
//		MappingBinding sblockmap = node2sBlock.get(subject);
//		if (sblockmap == null) {
//			// initialize the map
//			sblockmap = new MappingBinding(mappingConf, triples);
//			
//
//			node2sBlock.put(subject, sblockmap);
//		}
//
////		// add all the triples with their potential mappings to the s-block
////		for (Triple triple : triples) {
////			String pUri = getUri(Var.alloc(triple.getPredicate().getName()));
////			sblockmap.addP( triple.getObject().getName(), pUri);
////		}
////
////		sblockmap.cleanUp();
////		throw new ImplementationException("Adopt to R2RML");
////		// process the subject information
////		String sUri = getUri(Var.alloc(subject.getName()));
////		if (sUri != null) {
////			Collection<String> ldps = mappingConf.getPathForInstanceUri(sUri);
////			sblockmap.retainLdps(ldps);
////		}
//
//
//		//TODO "Adopt to R2RML");
//		
//		
////		for (Triple triple : triples) {
////			String oUri = getUri(Var.alloc(triple.getObject().getName()));
////			if (oUri != null) {
////				Collection<String> targetLdpsOfObject = mappingConf
////						.getPathForInstanceUri(oUri);
////
////				Multimap<String,TripleMap> toRetain = HashMultimap.create();
////
////				// we check if the mappings we already got.
////				for (String ldp : sblockmap.getLdps()) {
////
////					Set<Mapping> mappings = sblockmap.getMappings(ldp);
////					for (Mapping mapping : mappings) {
////
////						for (TripleMap coldef : mapping
////								.getColDefinitions()) {
////							//do not take the id column into account, as it cannot be in the object position.
////							if(!coldef.isIdColumn()){
////							// now check if they got anything in common
////							if (coldef.getJoinsAlias() != null) {
////								for (Mapping joinswithMapping : mappingConf.getJoinsWith(coldef)) {
////									String joinswithldp = joinswithMapping
////											.getIdColumn().getLinkedDataPath();
////									if (targetLdpsOfObject
////											.contains(joinswithldp)) {
////										// seems so
////										toRetain.put(triple.getObject().getName(),coldef);
////										break;
////
////									}
////								}
////							}
////							}
////							if(coldef.getUriTemplate()!=null){
////								String template = coldef.getLinkedDataPath();
////								// we check if the uri template is able to produce such an uri
////								
////								if(template.equals("*")){
////									//the generic template, this always matches
////									toRetain.put(triple.getObject().getName(),coldef);
////								}else if(oUri.startsWith(template)){
////									toRetain.put(triple.getObject().getName(),coldef);
////								}
////								
////							}
////
////						}
////
////					}
////
////				}
////				sblockmap.retainColDefs(toRetain);
////			}
////		}
////
////		sblockmap.cleanUp();
//
//
//	}
//
//	public List<TripleMap> getColumnDefinition(Var var) {
//		return null;
//	}
//
//	@Override
//	public String toString() {
//		return toString(0);
//	}
//
//	public String toString(int indent) {
//		String pre = "\n";
//		for (int i = 0; i < indent; i++) {
//			pre += "++";
//		}
//		StringBuffer sb = new StringBuffer();
//		// add information about itself
//		// print out ops
//		sb.append(pre + "Ops: ");
//		for (Op op : ops) {
//			String opString = op.toString();
//			sb.append(opString.substring(1, opString.indexOf("\n")));
//		}
//
//		pre += "*";
//		// print out Vars
//		sb.append(pre + "Vars: ");
//		for (Var var : getDefinedVars()) {
//			sb.append(var.getName() + "  ");
//		}
//
//		// print out filters
//		sb.append(pre + "Filters: ");
//		for (Expr filter : getFilterFor((Var[]) new ArrayList(getDefinedVars())
//				.toArray(new Var[0]))) {
//			sb.append(filter.toString() + "  ");
//		}
//
//		// print out filters
//		sb.append(pre + "Triples: ");
//		for (Triple triple : getTriples()) {
//			sb.append(triple.toString() + "  ");
//		}
//
//		// print out filters
//		sb.append(pre + "Mapping Candidates: ");
//		for (MappingBinding sblockmap : node2sBlock.values()) {
//			sb.append(sblockmap.toString());
//		}
//
//		sb.append("\n");
//
//		// add the info from the childs
//		for (ScopeBlock child : children) {
//			sb.append(child.toString((indent + 1)));
//		}
//
//		return sb.toString();
//	}
//
//	/**
//	 * checks if the variable at this point has an equals Uri statement
//	 * 
//	 * @param var
//	 * @param op
//	 * @return
//	 */
//	public String getUri(Var var) {
//		Set<Expr> uriCand = getFilterFor(var);
//
//		for (Expr expr : uriCand) {
//			if (expr instanceof E_Equals) {
//				E_Equals ee = (E_Equals) expr;
//				
//	
//					
//					
//					ExprVar eqVar  = ee.getArg1().getExprVar();
//					NodeValue eqUri = ee.getArg2().getConstant();
//					//try other way around;
//					if(eqVar == null&& eqUri == null){
//						eqVar = ee.getArg2().getExprVar();
//						eqUri = ee.getArg1().getConstant();
//					}
//					
//					if(eqVar != null && eqUri != null && eqUri instanceof NodeValueNode && eqUri.getNode().isURI()){
//						String uri = ((NodeValueNode)eqUri).getNode().getURI(); 
//						return uri ;
//					}
//
//					
//				}
//				
//
//			
//
//		}
//
//		// throw new ImplementationException("not yet implemented");
//
//		return null;
//
//	}
//
//	public MappingBinding getsBlock(Node s) {
//		return node2sBlock.get(s);
//	}
//
//	public void joinThem() {
//		
//		
//		
//		
//		
//		
//		
//		
//
////		Set<Triple> allTriples = new HashSet<Triple>();
////		allTriples.addAll(pushedInTriples);
////		allTriples.addAll(triples);
////		boolean hasChanged = false;
////		do {
////
////			// we check if any variable is used in different s-blocks as subject
////			// and object.
////
////			
////
////			for (Triple triple : allTriples) {
////				if (node2sBlock.containsKey(triple.getObject())) {
////					// triple is the triple, where the object is used somewhere
////					// else as subject
////
////					SBlockNodeMapping sbmapIsSubject = node2sBlock.get(triple
////							.getObject());
////					SBlockNodeMapping sbmapIsOjbect = node2sBlock.get(triple
////							.getSubject());
////					// we now get all Columns, where the
////					// var is in the object position and check where they are
////					// pointing to
////
////					// we create a set of all the ldps, that can be reached
////					// where sbmapisobkect is pointing
////					
////					throw new ImplementationException("Adopt to R2RML");
////
//////					Collection<TripleMap> pointingCols = sbmapIsOjbect
//////							.getColumn(triple.getObject().getName());
//////					Set<String> ldps = new HashSet<String>();
//////					for (TripleMap coldef : pointingCols) {
//////						if (coldef.getJoinsAlias() != null) {
//////
//////							for (Mapping map : mappingConf.getJoinsWith(coldef)) {
//////								ldps.add(map.getIdColumn().getLinkedDataPath());
//////							}
//////						}
//////					}
////
////					// as only the ldps that these cols point to can be sensibly
////					// joined, we can throw away the rest
//////					hasChanged = sbmapIsSubject.retainLdps(ldps);
////
////				}
////			}
////		} while (hasChanged);
//
//	}
//	
//	public static Map<Node, Set<Triple>> createSBlock(List<Triple> triples) {
//		return createSBlock(new HashSet(triples));
//	}
//	
//	public static Map<Node, Set<Triple>> createSBlock(Set<Triple> triples) {
//		Map<Node, Set<Triple>> triplesBySubject = new HashMap<Node, Set<Triple>>();
//
//		for (Triple triple : triples) {
//			Node subject = triple.getSubject();
//			if (!triplesBySubject.containsKey(subject)) {
//				triplesBySubject.put(subject, new HashSet<Triple>());
//			}
//			triplesBySubject.get(subject).add(triple);
//
//		}
//		return triplesBySubject;
//	}
//
//}
