package org.aksw.sparqlmap.config.syntax.r2rml;

import org.aksw.sparqlmap.config.syntax.TermCreator;

public class TermMapTemplate extends TermMap {
	
	String template;

	public TermMapTemplate(String template, int  type, TripleMap triMap) {
		super(triMap);
		this.template = template;
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
