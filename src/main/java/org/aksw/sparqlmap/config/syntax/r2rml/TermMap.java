package org.aksw.sparqlmap.config.syntax.r2rml;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import org.aksw.sparqlmap.config.syntax.ColumDefinition;
import org.aksw.sparqlmap.config.syntax.TermCreator;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ColumnHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.RDFNode;

public abstract class TermMap {
	TripleMap tm;
	public TermMap(TripleMap tm) {
		this.tm = tm;
	}

	public boolean isResource() {
		return getType() == ColumnHelper.COL_TYPE_RESOURCE ? true : false;
	}

	public boolean isBlank() {
		return getType() == ColumnHelper.COL_TYPE_BLANK ? true : false;
	}

	public boolean isLiteral() {
		return getType() == ColumnHelper.COL_TYPE_LITERAL ? true : false;
	}

	public boolean isCompatible(String iri) {
		throw new ImplementationException("Not implemented");
	}

	public boolean isCompatible(Pattern iri) {
		throw new ImplementationException("Not implemented");
	}

	public abstract int getType();

	public abstract TermCreator getTC();

	public abstract void toTtl(StringBuffer ttl);
	
	
	

}
