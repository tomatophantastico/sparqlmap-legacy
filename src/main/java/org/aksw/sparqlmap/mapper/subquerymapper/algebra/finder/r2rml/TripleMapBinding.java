package org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml;

import java.util.Set;

import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;

import com.hp.hpl.jena.graph.Triple;

public class TripleMapBinding {
	
	
	Triple triple;
	
	Set<SPO> candidates;
	
	
	
	
	/**
	 * the small version of a triple map, 
	 * @author joerg
	 *
	 */
	public class SPO{
		TermMap s;
		TermMap p;
		TermMap o;
		
	}
	
	
	
	
	public static Set<TripleMap> asTripleMaps(Set<SPO> spos){
		
		
		return null;
	}
	
	

}
