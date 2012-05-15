package org.aksw.sparqlmap.config.syntax.r2rml;

import org.aksw.sparqlmap.config.syntax.TermCreator;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class TermMapConstant extends TermMap{

	
	private RDFNode node;

	public TermMapConstant(RDFNode node,  RDFDatatype datatype, TripleMap triMap ) {
		super(triMap);
		this.node = node;
	}

	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public TermCreator getTC() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void toTtl(StringBuffer ttl) {
		// TODO Auto-generated method stub
		
	}
	
	
}
