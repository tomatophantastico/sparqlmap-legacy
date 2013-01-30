package org.aksw.sparqlmap.mapper.finder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.mapper.translate.ImplementationException;
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