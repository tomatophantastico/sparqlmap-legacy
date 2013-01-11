package org.aksw.sparqlmap;

import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.db.IDBAccess;
import org.aksw.sparqlmap.db.SQLResultSetWrapper;
import org.aksw.sparqlmap.mapper.Mapper;
import org.aksw.sparqlmap.mapper.translate.ImplementationException;
import org.openjena.riot.out.NQuadsWriter;
import org.openjena.riot.out.NTriplesWriter;
import org.openjena.riot.system.JenaWriterRdfJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Statement;
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
	private IDBAccess dbConf;	

	private Logger log = LoggerFactory.getLogger(SparqlMap.class);

	public enum ReturnType {JSON,XML}
	
	/**
	 * Takes some of the functionality of QueryExecutionbase
	 * @param query this gets executed
	 * @return the result as a string
	 * @throws SQLException 
	 */
	public void  executeSparql(String qstring, ReturnType rt, OutputStream out) throws SQLException{
		
		
		
		
		Query query = QueryFactory.create(qstring);
		if(query.isAskType()){
			throw new ImplementationException("Dont Ask");
		}
		if(query.isConstructType()){
			Model model = executeConstruct(rt,  query, com.hp.hpl.jena.sparql.graph.GraphFactory.makeJenaDefaultModel());
			writeModel(rt, out, model);
			
			
		}
		if(query.isSelectType()){
			SQLResultSetWrapper rs = executeSparql(query);
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
				SQLResultSetWrapper rs = executeSparql(query);
				while(rs.hasNext()){
					Binding b = rs.nextBinding();
					iris.add(b.get(var));
				}
				
			}
			
			for (Node node : iris) {
				String con1 = "CONSTRUCT {?s ?p <"+node.getURI()+">} WHERE {?s ?p <"+node.getURI()+">}";
				executeConstruct(rt, QueryFactory.create(con1), model);
				String con2 = "CONSTRUCT {<"+node.getURI()+"> ?p ?o} WHERE {<"+node.getURI()+"> ?p ?o}";
				executeConstruct(rt, QueryFactory.create(con2),model);
			}
			writeModel(rt, out, model);
			
		}

		

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


	private Model executeConstruct(ReturnType rt, Query query, Model model)
			throws SQLException {
		//take the graph pattern and convert it into a select query.
		Template template = query.getConstructTemplate();
		query.setQueryResultStar(true);
		//execute it 
		SQLResultSetWrapper rs = executeSparql(query);
		
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
		return model;
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
			SQLResultSetWrapper rs = dbConf.executeSQL(query);
			rs.setBaseUri(baseUri);
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
			rs.close();
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
			SQLResultSetWrapper rs = dbConf.executeSQL(query);
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
	
	
	public SQLResultSetWrapper executeSparql(Query query) throws SQLException{

		String sql = mapper.rewrite(query);
		return dbConf.executeSQL(sql);
		
	}

	public Mapper getMapper() {
		return mapper;
	}
}
