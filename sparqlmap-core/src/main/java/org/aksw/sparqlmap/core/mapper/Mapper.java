package org.aksw.sparqlmap.core.mapper;


import java.util.List;

import org.aksw.sparqlmap.core.TranslationContext;

import com.hp.hpl.jena.query.Query;

public interface Mapper {


	 /** transforms  a sparql select query into sql
	 * 
	 * @param Sparql
	 * @return
	 */
	public abstract void rewrite(TranslationContext context);
	


	public abstract List<TranslationContext> dump();

}