//package org.aksw.sparqlmap.web.servlets;
//
//import java.io.IOException;
//import java.sql.SQLException;
//
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//
//import com.hp.hpl.jena.sparql.core.DatasetGraph;
//
//public class DumpServlet extends AbstractSparqlMapServlet{
//	
//	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DumpServlet.class);
//	
//	
//	DatasetGraph dump = null;
//	long dumpCreationTimestamp;
//
//	@Override
//	public void doExecute(HttpServletRequest req, HttpServletResponse resp) {
//		
//		try {
//			sparqlmap.dump(resp.getOutputStream());
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			log.error("Error:",e);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			log.error("Error:",e);
//		}
//		
//		
//   
//		
//	}
//
//}
