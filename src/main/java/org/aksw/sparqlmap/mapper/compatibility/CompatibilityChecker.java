package org.aksw.sparqlmap.mapper.compatibility;

import java.util.Collection;

import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.expr.Expr;

public interface CompatibilityChecker {
	
	
	
	boolean isCompatible(Node n);
	
	boolean isCompatible(TermMap tm);

	boolean isCompatible(String var, Collection<Expr> oxprs);

}
