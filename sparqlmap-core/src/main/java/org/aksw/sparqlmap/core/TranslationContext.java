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
import com.hp.hpl.jena.sparql.algebra.Op;

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
	private String target = null;
	
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
	
	public String getTargetContentType() {
		return target;
	}
	
	public void setTargetContentType(String target) {
		this.target = target;
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
			sw = new Stopwatch();
			sw.start();
			
		}else{
			sw.stop();
			phaseDurations.put(currentPhase, sw.elapsedTime(TimeUnit.MICROSECONDS));
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
		phaseDurations.put(currentPhase, sw.elapsedTime(TimeUnit.MICROSECONDS));

	}

	
	
	
	

}
