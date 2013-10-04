package org.aksw.sparqlmap.core;

import java.util.LinkedHashMap;
import java.util.Map;

import org.aksw.commons.util.StopWatch;
import org.aksw.sparqlmap.core.mapper.finder.QueryInformation;
import org.slf4j.profiler.Profiler;
import org.slf4j.profiler.ProfilerRegistry;

import com.google.common.base.Stopwatch;
import com.hp.hpl.jena.graph.query.Mapping;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;

/**
 * This class holds all information needed for a specific translation.
 * 
 * @author joerg
 *
 */
public class TranslationContext {
	
	
	
	
	String queryString;
	
	// queryidentifier, for example to denote query type in  benchmarking run.
	String queryName;
	
	Query query;
	
	Op queryOp;
	
	QueryInformation queryInformation;
	
	Throwable problem;

	
	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public String getQueryName() {
		return queryName;
	}

	public void setQueryName(String queryName) {
		this.queryName = queryName;
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public Op getQueryOp() {
		return queryOp;
	}

	public void setQueryOp(Op queryOp) {
		this.queryOp = queryOp;
	}

	public QueryInformation getQueryInformation() {
		return queryInformation;
	}

	public void setQueryInformation(QueryInformation queryInformation) {
		this.queryInformation = queryInformation;
	}

	public Throwable getProblem() {
		return problem;
	}

	public void setProblem(Throwable problem) {
		this.problem = problem;
	}
	
	
	
	
	
	
	
	
	

}
