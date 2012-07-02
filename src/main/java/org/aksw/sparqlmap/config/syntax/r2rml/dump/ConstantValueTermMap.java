package org.aksw.sparqlmap.config.syntax.r2rml.dump;

import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ColumnHelper;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

public class ConstantValueTermMap extends TermMap {
	
	TripleMap triMap = null;
	Resource r = null;
	Literal l = null;
	
	
	public ConstantValueTermMap(TripleMap triMap, Resource r) {
		super(triMap);
		this.r = r;
		this.triMap = triMap;
	}
	public ConstantValueTermMap(TripleMap triMap, Literal l) {
		super(triMap);
		this.l = l;
		this.triMap = triMap;
	}
	
	
	@Override
	public int getType() {
		return (this.r == null)?ColumnHelper.COL_TYPE_LITERAL:ColumnHelper.COL_TYPE_RESOURCE;
	}
	
	@Override
	public TermCreator getTC() {
		
		return null;
	}

	@Override
	public void toTtl(StringBuffer ttl) {
		ttl.append("  rr:constant ");
		if(this.isResource()){
			ttl.append("<"+r.getURI()+">");
		}else{
			ttl.append("\""+l.getLexicalForm()+"\"");
		}
		
		
	}
	
	
	

}
