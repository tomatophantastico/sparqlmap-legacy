package org.aksw.sparqlmap.config.syntax.r2rml;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;

import com.hp.hpl.jena.rdf.model.Resource;

public class TermMapFactory {
	
	DataTypeHelper dth;
	
	
	public TermMap createConstantTermMap(Resource rs);
	public TermMap createConstantTermMap(Literal lit);
	public TermMap createColumnTermMap(String colname, int type);
	public TermMap createTemplateTermMap(String template, int type);
	
	

}
