package org.aksw.sparqlmap.mapper;


import org.aksw.sparqlmap.db.Connector;

import com.hp.hpl.jena.query.Query;

public interface Mapper {


	 /** transforms  a sparql select query into sql
	 * 
	 * @param Sparql
	 * @return
	 */
	public abstract String rewrite(Query sparql);

}