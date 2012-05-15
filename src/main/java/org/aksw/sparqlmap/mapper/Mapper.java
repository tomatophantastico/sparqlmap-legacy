package org.aksw.sparqlmap.mapper;

import org.aksw.sparqlmap.config.syntax.MappingConfiguration;
import org.aksw.sparqlmap.db.Connector;
import org.aksw.sparqlmap.db.SQLAccessFacade;

import com.hp.hpl.jena.query.Query;

public interface Mapper {

	public abstract void setConn(SQLAccessFacade conn);

	public abstract void setMappingConf(MappingConfiguration mappingConf);

	/**
	 * transforms  a sparql select query into sql
	 * 
	 * @param Sparql
	 * @return
	 */
	public abstract String rewrite(Query sparql);

}