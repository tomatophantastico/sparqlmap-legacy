package org.aksw.sparqlmap.config.syntax.r2rml;

import org.aksw.sparqlmap.config.syntax.TermCreator;

import net.sf.jsqlparser.schema.Column;

import com.hp.hpl.jena.rdf.model.RDFNode;

public class TermMapColumn extends TermMap {
	
	private Column col;

	public TermMapColumn(Column col, int nodeType, TripleMap triMap) {
		super(triMap);
		this.col = col;
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
