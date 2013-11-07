package org.aksw.sparqlmap.core;

import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.aksw.sparqlmap.core.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.Mapper;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.sparql.resultset.ResultsFormat;
import com.hp.hpl.jena.sparql.syntax.Template;

@Component
public class SparqlMap {
	
	// total queries translated by this instance
	private int querycount = 0;

	
	String baseUri;
	boolean continueWithInvalidUris = true;
	
	@PostConstruct
	public void loadBaseUri(){
		baseUri = env.getProperty("sm.baseuri");
		continueWithInvalidUris = new Boolean(env.getProperty("sm.continuewithinvaliduris","true"));
	}

	@Autowired
	public Environment env;
	
	

	@Autowired
	public Mapper mapper;
	
	@Autowired
	private R2RMLModel mapping;
	
	@Autowired
	private DBAccess dbConf;
	
	@PreDestroy
	public void profile(){
		Logger perfLog = LoggerFactory.getLogger("performance");
		List<String> queries =new ArrayList<String>(profReg.keySet());
		Collections.sort(queries);
		for(String query : queries){
			perfLog.debug("for query: " + query);
			List<String> execparts =new ArrayList<String>(profReg.get(query).keySet());
			Collections.sort(execparts);
			for(String execpart: execparts){
				List<Long> intDurations = new ArrayList<Long>(profReg.get(query).get(execpart));
				double[] dura = new double[intDurations.size()];
				for (int i = 0; i<intDurations.size();i++) {
					dura[i] = intDurations.get(i);
				}	
				perfLog.debug(String.format("%-20s :", execpart)+  StatUtils.geometricMean(dura));
				
			}
		}
	}
	
	private Map<String,Multimap<String, Long>> profReg = new HashMap<String, Multimap<String,Long>>();

	private Logger log = LoggerFactory.getLogger(SparqlMap.class);


	public void  executeSparql(String qstring, String rt, OutputStream out) throws SQLException{
		executeSparql(qstring,  rt,  out,"Unnamed query "+ this.querycount++);
	}
	
	
	
	
	
	
	
	
	
	
	
	/**
	 * Takes some of the functionality of QueryExecutionbase
	 * @param queryname 
	 * @param query this gets executed
	 * @return the result as a string
	 * @throws SQLException 
	 */
	public void  executeSparql(String qstring, String _rt, OutputStream out, String queryname) throws SQLException{
		
		TranslationContext context = new TranslationContext();
		context.setQueryString(qstring);
		context.setQueryName(queryname);
		
		
		context.profileStartPhase("Query Compile");
			
		
		context.setQuery(QueryFactory.create(qstring));
		
		context.setTargetContentType(_rt);
		
	
		
		if(context.getQuery().isAskType()){
			throw new ImplementationException("Dont Ask");
		}
		if(context.getQuery().isConstructType()){
			executeConstruct(context,out);
			
			
			
			
		}
		if(context.getQuery().isSelectType()){
			ResultSet rs = rewriteAndExecute(context);
			
			ResultSetFormatter.output(out, rs, ResultsFormat.lookup(WebContent.contentTypeToLang(context.getTargetContentType().toString()).getName()));
			
			
			
		}
		if(context.getQuery().isDescribeType()){
			Model model = 	com.hp.hpl.jena.sparql.graph.GraphFactory.makeJenaDefaultModel();
			List<Node> iris =  context.getQuery().getResultURIs();
			if((iris == null || iris.isEmpty())){
				Var var = context.getQuery().getProjectVars().get(0);
				/*
				// hacky, hacky, hacky
				String wherePart  = query.getQueryPattern().toString();
				
				String newwhere =wherePart.replaceAll("\\?"+var.toString()+"(?![a-zA-Z0-9])", "?x_sm");
				Query con = QueryFactory.create("CONSTRUCT {?s_sm ?p_sm ?o_sm} WHERE {{?s_sm ?p_sm ?x_sm. " +newwhere + "}UNION {?x_sm ?p_sm ?o_sm. "+newwhere+"}}");
				executeConstruct(rt,con,model,queryname);*/
				ResultSet rs = rewriteAndExecute(context);
				while(rs.hasNext()){
					iris.add(rs.next().get(var.getName()).asNode());
				}
				
				
			}
				
				for (Node node : iris) {
					String con1 = "CONSTRUCT {?s_sm ?p_sm <"+node.getURI()+"> } WHERE { ?s_sm ?p_sm <"+node.getURI()+"> }";
					TranslationContext subCon1 = new TranslationContext();
					subCon1.setTargetContentType(context.getTargetContentType());
					subCon1.setQueryString(con1);
					subCon1.setQueryName("construct incoming query");
					subCon1.setQuery(QueryFactory.create(con1));
					
					executeConstruct(subCon1,out);
					String con2 = "CONSTRUCT { <"+node.getURI()+"> ?p_sm ?o_sm} WHERE { <"+node.getURI()+"> ?p_sm ?o_sm}";
					TranslationContext subCon2 = new TranslationContext();
					subCon2.setTargetContentType(context.getTargetContentType());
					subCon2.setQueryString(con2);
					subCon2.setQuery(QueryFactory.create(con2));
					subCon2.setQueryName("construct outgoinf query");
					
					executeConstruct(subCon2, out);

					}
			
//			writeModel(rt, out, model);
			
		}

		

	}


	private Multimap<String, Long> getProfiler(String queryname) {
		Multimap<String, Long> prof = null;
		if(queryname!=null&&!queryname.isEmpty()){
			prof = profReg.get(queryname);
			if(prof==null){
				prof = HashMultimap.create();
				profReg.put(queryname,prof);
			}
		}
		return prof;
	}


	
	
	

	private void executeConstruct(TranslationContext context, OutputStream out)
			throws SQLException {
		//take the graph pattern and convert it into a select query.
		Template template = context.getQuery().getConstructTemplate();
		context.getQuery().setQueryResultStar(true);
		//execute it 
		ResultSet rs = rewriteAndExecute(context);
		
		//bind it
		int i = 0;
		Graph graph = GraphFactory.createDefaultGraph();
		while (rs.hasNext()) {
			Set<Triple> set = new HashSet<Triple>();
			Map<Node, Node> bNodeMap = new HashMap<Node, Node>();
			Binding binding = rs.nextBinding();
			template.subst(set, bNodeMap, binding);
			
			for (Triple t : set) {
				graph.add(t);
				
			}
			
			if(++i%1000!=0){
				RDFDataMgr.write(out, graph, WebContent.contentTypeToLang(context.getTargetContentType().toString()));
			}
		}
		RDFDataMgr.write(out, graph, WebContent.contentTypeToLang(context.getTargetContentType().toString()));

	}
	
	/**
	 * dumps into the whole config into the writer.
	 * @param writer
	 * @throws SQLException 
	 */

	public void dump(OutputStream out,String format) throws SQLException{
		PrintStream writer = new PrintStream(out);
		
		List<String> queries = mapper.dump();
		for (String query : queries) {
			TranslationContext context = new TranslationContext();
			context.setSqlQuery(query);
			context.setQueryName("Dump query");
			
			log.info("SQL: " + query);
			com.hp.hpl.jena.query.ResultSet rs = dbConf.executeSQL(context, baseUri);
			DatasetGraph graph = DatasetGraphFactory.createMem();
			boolean usesGraph = false;
			int i = 0;
			while(rs.hasNext()){
				Binding bind = rs.nextBinding();	
				if(bind.get(Var.alloc("g"))!=null){
					
					usesGraph =true;
				}
				try {
					Quad toadd = new Quad(bind.get(Var.alloc("g")),bind.get(Var.alloc("s")), bind.get(Var.alloc("p")), bind.get(Var.alloc("o")));
					graph.add(toadd)	;
				} catch (Exception e) {
					
					log.error("Error:",e);
					if(!continueWithInvalidUris){
						throw new RuntimeException(e);
					}
				}
				if(++i%1000==0){
					if(usesGraph){
						RDFDataMgr.write(out, graph, RDFFormat.NQUADS);
						graph.deleteAny(null, null, null, null);
					}else{
						RDFDataMgr.write(out, graph.getDefaultGraph(),Lang.NTRIPLES);
						graph.deleteAny(null, null, null, null);

					}
				}
			}
			if(usesGraph){
				RDFDataMgr.write(out, graph,RDFLanguages.nameToLang(format));
			}else{
				RDFDataMgr.write(out, graph.getDefaultGraph(), RDFLanguages.nameToLang(format));
			}
			
			writer.flush();
		}
	}
	
	/**
	 * dumps into the whole config into the writer.
	 * @param writer
	 * @throws SQLException 
	 */

	public DatasetGraph dump() throws SQLException{
		
		DatasetGraph dataset = DatasetGraphFactory.createMem();

		List<String> queries = mapper.dump();
		for (String query : queries) {
			TranslationContext context = new TranslationContext();
			context.setSqlQuery(query);
			context.setQueryName("Dump query");

			log.info("SQL: " + query);
			com.hp.hpl.jena.query.ResultSet rs = dbConf.executeSQL(context, baseUri);
			while(rs.hasNext()){
				Binding bind = rs.nextBinding();	
				try {
					Quad toadd = new Quad(bind.get(Var.alloc("g")),bind.get(Var.alloc("s")), bind.get(Var.alloc("p")), bind.get(Var.alloc("o")));
					dataset.add(toadd)	;
				} catch (Exception e) {
					
					log.error("Error:",e);
					if(!continueWithInvalidUris){
						throw new RuntimeException(e);
					}
				}
			}
		}
		return dataset;
	}
	
	
	public ResultSet rewriteAndExecute(String query) throws SQLException{
		
		TranslationContext context = new TranslationContext();
		context.setQueryString(query);
		context.setQuery(QueryFactory.create(query));
		
		return rewriteAndExecute(context);
		
	}

	
	
	public ResultSet rewriteAndExecute(TranslationContext context) throws SQLException{
		
		
	
		
		context.profileStartPhase("Rewriting");	
		
		
		context.setSqlQuery(mapper.rewrite(context));
		
		LoggerFactory.getLogger("sqllog").info("SQL " + context.getQueryName() + " " + context.getSqlQuery() );
		
			
		ResultSet rs = dbConf.executeSQL(context,baseUri);
		
		return rs;
		
	}

	public Mapper getMapper() {
		return mapper;
	}
}
