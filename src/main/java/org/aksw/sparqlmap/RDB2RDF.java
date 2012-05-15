package org.aksw.sparqlmap;

import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.R2RConfiguration;
import org.aksw.sparqlmap.config.syntax.SimpleConfigParser;
import org.aksw.sparqlmap.db.SQLAccessFacade;
import org.aksw.sparqlmap.db.SQLResultSetWrapper;
import org.aksw.sparqlmap.mapper.Mapper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.AlgebraBasedMapper;
import org.openjena.riot.system.JenaWriterRdfJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.syntax.Template;
import com.hp.hpl.jena.sparql.util.ModelUtils;
import com.hp.hpl.jena.xmloutput.impl.Basic;

public class RDB2RDF {

	private Mapper mapper;
	
	private R2RConfiguration config;
	
	private SQLAccessFacade db;
	
	private Logger log = LoggerFactory.getLogger(RDB2RDF.class);
	
	
	public RDB2RDF(String configLocation ){
		
		SimpleConfigParser parser = new SimpleConfigParser();

		try {
			String configName = "bsbm.r2rml";
			ClassLoader cl = this.getClass().getClassLoader();
			URL u = cl.getResource(configName);
			if (u == null) u = ClassLoader.getSystemResource(configName);
						
			config = parser.parse(new InputStreamReader(u.openStream()));
			mapper = new AlgebraBasedMapper(config);

			db = new SQLAccessFacade(config.getDbConn());
		} catch (Exception e) {
			log.error("Error setting up the app", e);
		}

		
		
	}
	private static long sqltime = 0;
	private static long executetime= 0;
	
	
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
	
	
	private SQLResultSetWrapper executeSparql(Query query) throws SQLException{

		String sql = mapper.rewrite(query);
		return db.executeSQL(sql);
		
	}

	public Mapper getMapper() {
		return mapper;
	}
}
