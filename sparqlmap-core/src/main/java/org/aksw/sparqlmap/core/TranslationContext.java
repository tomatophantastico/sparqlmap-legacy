package org.aksw.sparqlmap.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.aksw.sparqlmap.core.mapper.finder.MappingBinding;
import org.aksw.sparqlmap.core.mapper.finder.QueryInformation;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.WebContent;

import com.google.common.base.Stopwatch;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.resultset.ResultsFormat;

/**
 * This class holds all information needed for a specific translation.
 * 
 * @author joerg
 * 
 */
public class TranslationContext {
	
	
	
	
	private String queryString;
	
	private String queryName;
	
	private MappingBinding queryBinding;

	private Query query;
	
	private Op beautifiedQuery;
	
	private QueryInformation queryInformation;
	
	private String sqlQuery;
	
	/*
	 * define if the result should be json/rdf, RDF/XML, turtle or else
	 */
	private Object target = null;
	
	private Throwable problem;
	
	public int subquerycounter = 0;
	
	public int aliascounter = 0 ; 
	
	public int duplicatecounter = 0;
	
	
	public Map<String, Long> phaseDurations = new LinkedHashMap<String, Long>();
	
	public MappingBinding getQueryBinding() {
		return queryBinding;
	}
	public void setQueryBinding(MappingBinding queryBinding) {
		this.queryBinding = queryBinding;
	}
	
	public Object getTargetContentType() {
		return target;
	}
	
	public void setTargetContentType(Object rf) {
		this.target = rf;
	}
	
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

	

	public QueryInformation getQueryInformation() {
		return queryInformation;
	}

	public void setQueryInformation(QueryInformation queryInformation) {
		this.queryInformation = queryInformation;
	}

	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	public Throwable getProblem() {
		return problem;
	}

	public void setProblem(Throwable problem) {
		this.problem = problem;
	}
	
	public Op getBeautifiedQuery() {
		return beautifiedQuery;
	}
	
	public void setBeautifiedQuery(Op beautifiedQuery) {
		this.beautifiedQuery = beautifiedQuery;
	}
	
	
	private Stopwatch sw;
	private String currentPhase;

	public void profileStartPhase(String phase) {
		if(sw == null){
			currentPhase = phase;
			sw = Stopwatch.createStarted();
			
		}else{
			sw.stop();
			phaseDurations.put(currentPhase, sw.elapsed(TimeUnit.MICROSECONDS));
			currentPhase = phase;
			sw.reset();
			sw.start();
		}
		
	}
	
	public void profileStop(){
		if(sw==null){
			throw new UnsupportedOperationException("Has first to be started");
		}
		sw.stop();
		phaseDurations.put(currentPhase, sw.elapsed(TimeUnit.MICROSECONDS));

	}
	
	
	
	@Override
	public String toString() {
		
		StringBuffer sb = new StringBuffer();
		sb.append("Translation Context for: \n");
		
		if(queryName!=null){
			sb.append("\nQueryname: " + queryName);
		}
		sb.append("\nSPARQL Query: " );
		if(queryString!=null){
			sb.append(queryString);
		}else{
			sb.append(query.toString(Syntax.defaultQuerySyntax));
		}
		
		if(problem!=null){
			sb.append("\nProblem: " + problem.getLocalizedMessage());
		}
		
		return sb.toString();
	}

	
	
	
	

}
