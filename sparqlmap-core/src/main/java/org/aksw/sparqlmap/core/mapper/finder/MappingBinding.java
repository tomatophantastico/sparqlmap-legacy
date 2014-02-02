package org.aksw.sparqlmap.core.mapper.finder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap.PO;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 * this class cis a wrapper around the triple bindings
 * 
 * @author joerg
 * 
 */
public class MappingBinding {

	private Map<Quad, Collection<TripleMap>> bindingMap = new HashMap<Quad, Collection<TripleMap>>();


	public MappingBinding(Map<Quad, Collection<TripleMap>> bindingMap) {
		this.bindingMap = bindingMap;
	}


	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Triple Bindings are: \n");
		Set<Quad> quads = this.bindingMap.keySet();
		for (Quad quad: quads) {
			sb.append("* " + quad.toString() + "\n");
			for (TripleMap tm : this.bindingMap.get(quad)) {
				sb.append("    Triplemap: " + tm + "\n");
				for (PO po : tm.getPos()) {
					sb.append("     PO:" + po.getPredicate().toString() + " "
							+ po.getObject().toString() + "\n");
				}
			}
		}
		return sb.toString();
	}
	
	
	public Map<Quad, Collection<TripleMap>> getBindingMap() {
		return bindingMap;
	}
}