package org.aksw.sparqlmap.web.servlets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.aksw.sparqlmap.SparqlMap.ReturnType;

public class SPARQLServlet extends AbstractSparqlMapServlet{
	
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SPARQLServlet.class);
		

	
	
	
	@Override
	public void doExecute(HttpServletRequest req, HttpServletResponse resp){
	
		
		try {
			
			String query = req.getParameter("query");
			String outputformat = req.getParameter("output");
			log.debug("Receveived query: " + query);
			try {

				if(outputformat!=null && outputformat.contains("json")){
					resp.setContentType("application/sparql-results+json");
					ByteArrayOutputStream bio = new ByteArrayOutputStream();
					
					sparqlmap.executeSparql(query, ReturnType.JSON,bio);
					resp.getWriter().append(bio.toString());
					
				}else{
					resp.setContentType("application/sparql-results+xml");
					sparqlmap.executeSparql(query, ReturnType.XML, resp.getOutputStream());
					
				}
			} catch (SQLException e) {
				
				resp.getOutputStream().write(e.getMessage().getBytes());
				log.error("Error for query \n" + query + "\n",e);
			}
		} catch (IOException e) {
			log.error("Error:",e);
		} catch (Throwable t){
			log.error("Throwable caught: ", t);
		
		}
		
		
	}
	
	
	@Override
	public void destroy() {
		super.destroy();
	}

}
