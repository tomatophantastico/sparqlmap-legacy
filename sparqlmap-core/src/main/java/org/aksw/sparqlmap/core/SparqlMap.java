package org.aksw.sparqlmap.core;

import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.aksw.sparqlmap.core.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.db.DeUnionResultWrapper;
import org.aksw.sparqlmap.core.db.SQLResultSetWrapper;
import org.aksw.sparqlmap.core.mapper.Mapper;
import org.aksw.sparqlmap.core.mapper.translate.ImplementationException;
import org.apache.commons.math3.stat.StatUtils;
import org.openjena.riot.out.NQuadsWriter;
import org.openjena.riot.out.NTriplesWriter;
import org.openjena.riot.system.JenaWriterRdfJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.profiler.Profiler;
import org.slf4j.profiler.ProfilerRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.syntax.Template;
import com.hp.hpl.jena.sparql.util.ModelUtils;
import com.hp.hpl.jena.xmloutput.impl.Basic;

@Component
public class SparqlMap {
	
	
	String baseUri;
	boolean continueWithInvalidUris = true;

//	public SparqlMap(String baseUri) {
//		super();
//		this.baseUri = baseUri;
//	}
	@Autowired
	public Environment env;
	
	@PostConstruct
	public void loadBaseUri(){
		baseUri = env.getProperty("sm.baseuri");
		continueWithInvalidUris = new Boolean(env.getProperty("sm.continuewithinvaliduris","true"));
	}

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

	public enum ReturnType {JSON,XML}
	
	/**
	 * Takes some of the functionality of QueryExecutionbase
	 * @param queryname 
	 * @param query this gets executed
	 * @return the result as a string
	 * @throws SQLException 
	 */
	public void  executeSparql(String qstring, ReturnType rt, OutputStream out, String queryname) throws SQLException{
		
		Multimap<String, Long> prof = getProfiler(queryname);
		Stopwatch sw = new Stopwatch();
		
		if(prof!=null){
			sw= new Stopwatch().start();
		}
			
		
		Query query = QueryFactory.create(qstring);
		
		if(prof!=null){
			sw.stop();
			prof.put("0 Parse", sw.elapsedTime(TimeUnit.MICROSECONDS));
		}
		
		if(query.isAskType()){
			throw new ImplementationException("Dont Ask");
		}
		if(query.isConstructType()){
			Model model = com.hp.hpl.jena.sparql.graph.GraphFactory.makeJenaDefaultModel();
			executeConstruct(rt,  query,model ,queryname);
			writeModel(rt, out, model);
			
			
		}
		if(query.isSelectType()){
			ResultSet rs = executeSparql(query,queryname);
			switch (rt) {
			case JSON:
				ResultSetFormatter.outputAsJSON(out,rs);
				break;

			default:
				ResultSetFormatter.outputAsXML(out,rs);
				break;
			}
			
		}
		if(query.isDescribeType()){
			Model model = 	com.hp.hpl.jena.sparql.graph.GraphFactory.makeJenaDefaultModel();
			List<Node> iris =  query.getResultURIs();
			if((iris == null || iris.isEmpty())){
				Var var = query.getProjectVars().get(0);
				/*
				// hacky, hacky, hacky
				String wherePart  = query.getQueryPattern().toString();
				
				String newwhere =wherePart.replaceAll("\\?"+var.toString()+"(?![a-zA-Z0-9])", "?x_sm");
				Query con = QueryFactory.create("CONSTRUCT {?s_sm ?p_sm ?o_sm} WHERE {{?s_sm ?p_sm ?x_sm. " +newwhere + "}UNION {?x_sm ?p_sm ?o_sm. "+newwhere+"}}");
				executeConstruct(rt,con,model,queryname);*/
				ResultSet rs = executeSparql(query, queryname);
				while(rs.hasNext()){
					iris.add(rs.next().get(var.getName()).asNode());
				}
				
				
			}
				
				for (Node node : iris) {
					String con1 = "CONSTRUCT {?s_sm ?p_sm <"+node.getURI()+"> } WHERE { ?s_sm ?p_sm <"+node.getURI()+"> }";
					executeConstruct(rt, QueryFactory.create(con1), model,queryname);
					String con2 = "CONSTRUCT { <"+node.getURI()+"> ?p_sm ?o_sm} WHERE { <"+node.getURI()+"> ?p_sm ?o_sm}";
					executeConstruct(rt, QueryFactory.create(con2), model,queryname);

					}
			
			writeModel(rt, out, model);
			
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


	private void writeModel(ReturnType rt, OutputStream out, Model model) {
		switch (rt) {
		case JSON:
			JenaWriterRdfJson writer = new JenaWriterRdfJson();
			writer.write(model, out, null);
			break;

		default:
			Basic xmlwriter = new Basic();
			xmlwriter.write(model, out, null);
			break;
		}
	}


	private void executeConstruct(ReturnType rt, Query query, Model model,String queryname)
			throws SQLException {
		//take the graph pattern and convert it into a select query.
		Template template = query.getConstructTemplate();
		query.setQueryResultStar(true);
		//execute it 
		ResultSet rs = executeSparql(query,queryname);
		
		//bind it

		while (rs.hasNext()) {
			Set<Triple> set = new HashSet<Triple>();
			Map<Node, Node> bNodeMap = new HashMap<Node, Node>();
			Binding binding = rs.nextBinding();
			template.subst(set, bNodeMap, binding);
			for (Triple t : set) {
				Statement stmt = ModelUtils.tripleToStatement(model, t);
				if (stmt != null)
					model.add(stmt);
			}
		}

	}
	
	/**
	 * dumps into the whole config into the writer.
	 * @param writer
	 * @throws SQLException 
	 */

	public void dump(OutputStream out) throws SQLException{
		PrintStream writer = new PrintStream(out);
		
		List<String> queries = mapper.dump();
		for (String query : queries) {
			log.info("SQL: " + query);
			com.hp.hpl.jena.query.ResultSet rs = dbConf.executeSQL(query, baseUri);
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
					NQuadsWriter.write(out, graph);
				}
			}
			if(usesGraph){
				NQuadsWriter.write(writer, graph);
			}else{
				NTriplesWriter.write(writer, graph.getGraph(null));
			}
			
			writer.flush();
		}
	}
	
	public DatasetGraph dump() throws SQLException{
		
		DatasetGraph graph = DatasetGraphFactory.createMem();

	
		List<String> queries = mapper.dump();
		for (String query : queries) {
			log.info("SQL: " + query);
			com.hp.hpl.jena.query.ResultSet rs = dbConf.executeSQL(query,baseUri);
			while(rs.hasNext()){
				Binding bind = rs.nextBinding();
				Node graphNode = null;
				if(bind.get(Var.alloc("g"))!=null){
					graphNode =bind.get(Var.alloc("g"));
				}else{
					graphNode = Quad.defaultGraphIRI;
				}
				graph.add(new Quad(graphNode,bind.get(Var.alloc("s")), bind.get(Var.alloc("p")), bind.get(Var.alloc("o"))))	;
			}

		}
		
		return graph;
	}
	
	int querycount = 0;
	public ResultSet executeSparql(Query query, String queryname) throws SQLException{
		Multimap<String, Long> prof= getProfiler(queryname);
		Stopwatch sw = null;
		if(prof!=null){
			sw = new Stopwatch().start();
		}
		
		
		
		String sql = mapper.rewrite(query);
		
		
		if(queryname==null){
			queryname = "";
		}
	
		LoggerFactory.getLogger("sqllog").info("SQL " + queryname + " " + sql );
		
		
		if(prof!=null){
			sw.stop();
			prof.put("1 Rewrite",sw.elapsedTime(TimeUnit.MICROSECONDS));
		}
		ResultSet rs = dbConf.executeSQL(sql,baseUri,prof);
		
		
	
		if(rs instanceof DeUnionResultWrapper){
			((DeUnionResultWrapper) rs).setProfiler(prof);
		}
		if(rs instanceof SQLResultSetWrapper){
			((SQLResultSetWrapper) rs).setProfiler(prof);
		}
		

		
		return rs;
		
	}

	public Mapper getMapper() {
		return mapper;
	}
}
