//package org.aksw.sparqlmap.web.servlets;
//
//import java.io.IOException;
//import java.sql.SQLException;
//
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//
//import com.hp.hpl.jena.query.Dataset;
//import com.hp.hpl.jena.query.Query;
//import com.hp.hpl.jena.query.QueryExecution;
//import com.hp.hpl.jena.query.QueryExecutionFactory;
//import com.hp.hpl.jena.query.QueryFactory;
//import com.hp.hpl.jena.query.ResultSet;
//import com.hp.hpl.jena.query.ResultSetFormatter;
//import com.hp.hpl.jena.rdf.model.Model;
//import com.hp.hpl.jena.sparql.core.DatasetGraph;
//import com.hp.hpl.jena.sparql.core.DatasetImpl;
//
//public class SPARQLDumpJenaServlet extends AbstractSparqlMapServlet {
//
//	static org.slf4j.Logger log = org.slf4j.LoggerFactory
//			.getLogger(SPARQLDumpJenaServlet.class);
//
//	DatasetGraph dump;
//	private long dumpCreationTimestamp = 0;
//	
//	
//	@Override
//	public void doExecute(HttpServletRequest req, HttpServletResponse resp) {
//		try {
//			if (dump == null
//					|| System.currentTimeMillis() > (dumpCreationTimestamp + 600000)) {
//
//				try {
//					dump = sparqlmap.dump();
//					dumpCreationTimestamp = System.currentTimeMillis();
//					
//				} catch (SQLException e) {
//					log.error("Error:", e);
//
//					resp.sendError(503, e.getMessage());
//
//				}
//			}
//
//			Query query = QueryFactory.create(req.getParameter("query"));
//
//			String outputformat = req.getParameter("output");
//
//			Dataset ds = DatasetImpl.wrap(dump);
//
//			QueryExecution exec = QueryExecutionFactory.create(query, ds);
//
//			if (query.isAskType()) {
//				boolean ask = exec.execAsk();
//				if (outputformat != null && outputformat.contains("json")) {
//					resp.setContentType("application/sparql-results+json");
//					ResultSetFormatter
//							.outputAsJSON(resp.getOutputStream(), ask);
//				} else {
//					resp.setContentType("application/sparql-results+xml");
//					ResultSetFormatter.outputAsXML(resp.getOutputStream(), ask);
//				}
//			} else if (query.isConstructType()) {
//				Model qresult = exec.execConstruct();
//
//				if (outputformat != null && outputformat.contains("json")) {
//					resp.setContentType("application/sparql-results+json");
//					ModelToJSON.getJSON(qresult);
//				} else {
//					resp.setContentType("application/sparql-results+xml");
//					qresult.write(resp.getOutputStream());
//				}
//			} else if (query.isDescribeType()) {
//				Model qresult = exec.execDescribe();
//
//				if (outputformat != null && outputformat.contains("json")) {
//					resp.setContentType("application/sparql-results+json");
//					ModelToJSON.getJSON(qresult);
//				} else {
//					resp.setContentType("application/sparql-results+xml");
//					qresult.write(resp.getOutputStream());
//				}
//			} else if (query.isSelectType()) {
//				ResultSet rs = exec.execSelect();
//				if (outputformat != null && outputformat.contains("json")) {
//					resp.setContentType("application/sparql-results+json");
//					ResultSetFormatter.outputAsJSON(resp.getOutputStream(), rs);
//				} else {
//					resp.setContentType("application/sparql-results+xml");
//					ResultSetFormatter.outputAsXML(resp.getOutputStream(), rs);
//				}
//			}
//
//		} catch (IOException e1) {
//			log.error("Error:", e1);
//		}
//	}
//
//}
