package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.ColumDefinition;
import org.aksw.sparqlmap.config.syntax.Mapping;
import org.aksw.sparqlmap.config.syntax.MappingConfiguration;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.core.Var;

public class MappingFinder {
	
	
	private Map<Op,Op> parentOfOp = new HashMap<Op, Op>();
	private LinkedHashMultimap<Op, Op> siblingsOfOp = LinkedHashMultimap.create();
	private Op root;
	
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(MappingFinder.class);

	MappingConfiguration mappingConf;
	private FilterFinder filterFinder;

	public MappingFinder(MappingConfiguration mappingConf,
			FilterFinder filterFinder, Op query) {
		this.mappingConf = mappingConf;
		this.filterFinder = filterFinder;
		this.root = query;
		opT = new MappingUtils.OpTree(root);
		parseQuery();
		
	}

	MappingUtils.OpTree opT;
	

	private void parseQuery() {
		
		
		OpVisitorBase bgpVisitor = new OpVisitorBase(){
			
			public void visit(OpBGP opBGP) {
				Set<Triple> triplesToCheck = new HashSet<Triple>(); 
				

				getMappingCandidates(opBGP).queriesFor(triplesToCheck);
				
				
			}
		};
		OpWalker.walk(root, bgpVisitor);
		
		

		
	}




	private Map<Op, MappingCandidates> op_candidates = new HashMap<Op, MappingCandidates>();
	private List<Var> projection = new ArrayList<Var>();



	public MappingCandidates getMappingCandidates(Op op) {

		MappingCandidates mc = op_candidates.get(op);
		if (mc == null) {
			mc = new MappingCandidates(op);
			op_candidates.put(op, mc);
		}
		return mc;
	}

	/**
	 * MappingCandidates allows the collection and compution of all possible
	 * Candidates for a mapping of a triple
	 * 
	 * @author joerg
	 * 
	 */
	public class MappingCandidates {
		
		private Op op;
		
		public MappingCandidates(Op op) {
			this.op = op;
			if(op instanceof OpBGP){
				OpBGP bgp  = (OpBGP) op;
				queriesFor(new HashSet<Triple>(bgp.getPattern().getList()));
			}
			
			
		}

		private boolean optimized = false;

		private Map<Node, Map<String, Set<Mapping>>> mappingsOfNode = new HashMap<Node, Map<String, Set<Mapping>>>();

		private void queriesFor(Set<Triple> triples) {
			// We first separate all triples according to their subjects.
			Map<Node, Set<Triple>> triplesBySubject = MappingUtils
					.createSBlock(triples);

			// we now iterate over for each s-triple block

			for (Node subject : triplesBySubject.keySet()) {
				Set<Triple> s_triples = triplesBySubject.get(subject);
				findMappings(subject, s_triples);

			}

		}

		/**
		 * 
		 * @param triples
		 *            with one subject
		 */
		private void findMappings(Node subject, Set<Triple> triples) {
			Map<String, Set<Mapping>> ldpMappings = mappingsOfNode.get(subject);
			if (ldpMappings == null) {
				// initialize the map
				ldpMappings = new HashMap<String, Set<Mapping>>();
				for (String ldp : mappingConf.getLinkedDataPaths()) {
					Set<Mapping> mappings = new HashSet<Mapping>();
					ldpMappings.put(ldp, new HashSet<Mapping>());
				}
				mappingsOfNode.put(subject, ldpMappings);
			}
			String sUri = filterFinder.getUri(Var.alloc(subject.getName()), op);
			if (sUri != null) {
				Collection<String> ldps = mappingConf
						.getPathForInstanceUri(sUri);
				ldpMappings.keySet().retainAll(ldps);
			}

			for (Triple triple : triples) {
				// Node predicate = triple.getPredicate();
				String pUri = filterFinder.getUri(
						Var.alloc(triple.getPredicate().getName()), op);
				String oUri = filterFinder.getUri(
						Var.alloc(triple.getObject().getName()), op);
				Node object = triple.getObject();

				if (pUri != null) {
					// we know now, which mapping is of the ldp is required
					for (String ldp : ldpMappings.keySet()) {
						Collection<Mapping> mappings = mappingConf
								.getMappings(ldp);

						for (Mapping mapping : mappings) {
							// check for if the mapping has a Property of that
							// iri
							if (mapping.getColumDefinition(pUri) != null) {
								// it has, so we add it
								ldpMappings.get(ldp).add(mapping);
							}
						}
					}

					if (object.isVariable()) {
						Collection<String> ldps = mappingConf
								.getPathForProperty(pUri);
						ldpMappings.keySet().retainAll(ldps);

					} else {
						log.warn("object uri lookup not implemented");

					}

				} else {
					// p is not defined, so we add all mappings.
					for (String ldp : ldpMappings.keySet()) {
						Collection<Mapping> mappings = mappingConf
								.getMappings(ldp);
						ldpMappings.get(ldp).addAll(mappings);

					}
				}
				
				

			}

			// remove all empty ldps

			Set<String> ldps_toRemove = new HashSet<String>();
			for (String ldp : ldpMappings.keySet()) {
				if (ldpMappings.isEmpty() || ldpMappings.get(ldp).isEmpty()) {
					ldps_toRemove.add(ldp);
				}
			}

			for (String ldp_toRemove : ldps_toRemove) {
				ldpMappings.remove(ldp_toRemove);
			}

		}

		@Override
		public String toString() {
			StringBuffer stringBuffer = new StringBuffer();

			for (Node node : mappingsOfNode.keySet()) {
				stringBuffer.append("\n" + node.toString() + ": ");

				for (String ldp : mappingsOfNode.get(node).keySet()) {
					stringBuffer.append("\n  ldp: " + ldp);

					for (Mapping mapping : mappingsOfNode.get(node).get(ldp)) {
						stringBuffer.append("\n         mapping: "
								+ mapping.getFromPart().toString());
					}
				}

			}

			return stringBuffer.toString();
		}

		public Map<Node, Map<String, Set<Mapping>>> getMappingsOfNode() {
			if (!optimized) {
				optimize();
				optimized = true;
			}
			return mappingsOfNode;
		}

		private void optimize() {
			// we go through all triples of this pattern 
			
			
			if(op instanceof OpBGP){
				OpBGP bgp = (OpBGP) op;
				
				
				Set<Triple> triples = new HashSet<Triple>(((OpBGP) op).getPattern().getList());
				
				//add all the other triples visible here
				for(Op op: opT.getVisiblefor(bgp)){
					if(op instanceof OpBGP){
						triples.addAll(((OpBGP) op).getPattern().getList());
					}
				}
				
				
				Multimap<String, Triple> var2Triple = LinkedHashMultimap.create();
				//we now add all variables to the map;
				
				for (Triple triple : triples) {
					var2Triple.put(triple.getSubject().getName(), triple);
					//vars.put(triple.getPredicate().getName(), triple); we don't consider predicates at the moment
					var2Triple.put(triple.getObject().getName(), triple);
				}
				
				
				List<String> donotcareabout = new ArrayList<String>();
				
				//now we throw away all the variables mentioned only once
				
				
				for (String var : var2Triple.keySet()) {
					if(var2Triple.get(var).size()==1){
						donotcareabout.add(var);
					}
				}
				
				//and the ones that are only used as subjects or objects are also thrown out
				for(String var : var2Triple.keySet()){
					int pos = 0;
					for(Triple triple: var2Triple.get(var)){
						if (pos==0){
							pos = pos(var,triple);
						}else{
							if(pos!=pos(var,triple)){
								pos =-1;
								break;
							}
						}
					}
					if(pos >= 0){
						donotcareabout.add(var);
					}
					
				}
				for (String string : donotcareabout) {
					var2Triple.removeAll(string);
				}
				// we got now a list of all variables mentioned in multiple patterns
				// we check them now for the ones that are somewhere objects with defined relations	
				for(String var : var2Triple.keySet()){
					Set<Mapping> joinswith = new HashSet<Mapping>();
					for(Triple triple : var2Triple.get(var)){
						if(triple.getObject().getName().equals(var)){
							String pUri = filterFinder.getUri(
									Var.alloc(triple.getPredicate().getName()), op);
							if(pUri!=null){
								for(String ldp: mappingsOfNode.get(triple.getObject()).keySet()){
									 Set<Mapping> mappings = mappingsOfNode.get(triple.getObject()).get(ldp);
									 for (Mapping mapping : mappings) {
										Collection<ColumDefinition> coldefs  = mapping.getColumDefinition(pUri);
										if(coldefs!=null){
										for(ColumDefinition coldef:coldefs){
											if(coldef.getJoinsAlias()!=null){
												joinswith.addAll(mappingConf.getJoinsWith(coldef));
											}
										}
										}
									}
								}
								
							}else{
								log.error("Optimization with ?p not implemented");
							}
						}
					}

					//in joinswith is now the information which mappings are potentially in the joinable mappings. so whenever the variable is used, in these mappings intersect with the previously defined
					
					for(Triple triple : var2Triple.get(var)){
						if(triple.getSubject().getName().equals(var)){
							Map<String,Set<Mapping>> ldpMappings = mappingsOfNode.get(triple.getSubject());
							List<String> retainLdps = new ArrayList<String>();
							for(Mapping map: joinswith){
								retainLdps.add(map.getIdColumn().getLinkedDataPath());
							}
							
							ldpMappings.keySet().retainAll(retainLdps);
							ldpMappings.size();
						}
					}
					
					
				}
				
				int i = 0;
				i++;

				
			}	
		}
		
		private int pos(String node,Triple triple){
			if(triple.getSubject().getName().equals(node)){
				return 1;
			}else if(triple.getPredicate().getName().equals(node)){
				return 2;
			}else if(triple.getObject().getName().equals(node)){
				return 3;
			}else {
				return 0;
			}
			
		}

	}

	@Override
	public String toString() {
		StringBuffer str = new StringBuffer();
		for (Op op : op_candidates.keySet()) {

			str.append(op.toString());
			str.append(" :");
			MappingCandidates cand = op_candidates.get(op);
			str.append(cand.toString());
		}

		return str.toString();

	}
	
	
	
	
	

}
