package org.aksw.sparqlmap.mapper.finder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap.PO;

import com.hp.hpl.jena.graph.Triple;

/**
 * this class cis a wrapper around the triple bindings
 * 
 * @author joerg
 * 
 */
public class MappingBinding {

	private Map<Triple, Collection<TripleMap>> bindingMap = new HashMap<Triple, Collection<TripleMap>>();


	public MappingBinding(Map<Triple, Collection<TripleMap>> bindingMap) {
		this.bindingMap = bindingMap;
	}


	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Triple Bindings are: \n");
		Set<Triple> triples = this.bindingMap.keySet();
		for (Triple triple : triples) {
			sb.append("* " + triple.toString() + "\n");
			for (TripleMap tm : this.bindingMap.get(triple)) {
				sb.append("    Triplemap: " + tm + "\n");
				for (PO po : tm.getPos()) {
					sb.append("     PO:" + po.getPredicate().toString() + " "
							+ po.getObject().toString() + "\n");
				}
			}
		}
		return sb.toString();
	}
	
	
	public Map<Triple, Collection<TripleMap>> getBindingMap() {
		return bindingMap;
	}
}