package net.sf.jsqlparser.statement.select;

import net.sf.jsqlparser.util.deparser.SelectDeParser;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

public class ExplicitSelectBody implements SelectBody{
	
	
	String query;
	
	public ExplicitSelectBody(String query) {
		this.query = query;
	}

	@Override
	public void accept(SelectVisitor selectVisitor) {
		if (selectVisitor instanceof SelectDeParser) {
			SelectDeParser dep = (SelectDeParser) selectVisitor;
			dep.visit(this);	
		}else{
			throw new ImplementationException("Use custom SElectDeParser");
		}
		
			
	}

	public Object getQuery() {
		return query;
	}

}
