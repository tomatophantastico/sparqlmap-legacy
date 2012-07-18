package org.aksw.sparqlmap.mapper;


import java.io.OutputStream;
import java.util.List;

import org.aksw.sparqlmap.RDB2RDF.ReturnType;

import com.hp.hpl.jena.query.Query;

public interface Mapper {


	 /** transforms  a sparql select query into sql
	 * 
	 * @param Sparql
	 * @return
	 */
	public abstract String rewrite(Query sparql);
	


	public abstract List<String> dump();

}