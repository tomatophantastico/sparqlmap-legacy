package org.aksw.sparqlmap.core.mapper;


import java.util.List;

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