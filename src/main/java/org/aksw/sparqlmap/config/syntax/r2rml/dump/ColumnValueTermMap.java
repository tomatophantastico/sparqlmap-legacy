package org.aksw.sparqlmap.config.syntax.r2rml.dump;

import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;

import net.sf.jsqlparser.schema.Column;


public class ColumnValueTermMap extends TermMap {
	
	
	boolean isResource;
	TripleMap triMap;
	
	public ColumnValueTermMap(Column col,TripleMap triMap, boolean isResource) {
		super(triMap);
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
