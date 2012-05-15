package org.aksw.sparqlmap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.aksw.sparqlmap.RDB2RDF.ReturnType;

public class SPARQLServlet extends HttpServlet{
	
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SPARQLServlet.class);
	
	RDB2RDF r2r = new RDB2RDF("bsbm.r2rml");
	
	 

	public SPARQLServlet() {

		
		
		
	}
	
	
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		doExecute(req, resp);
	}
	
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		doExecute(req, resp);
	}
	
	
	private void doExecute(HttpServletRequest req, HttpServletResponse resp){
	
		try {
			
			String query = req.getParameter("query");
			String outputformat = req.getParameter("output");
			log.debug("Receveived query: " + query);
			try {

				if(outputformat!=null && outputformat.contains("json")){
					resp.setContentType("application/sparql-results+json");
					ByteArrayOutputStream bio = new ByteArrayOutputStream();
					
					r2r.executeSparql(query, ReturnType.JSON,bio);
					resp.getWriter().append(bio.toString());
					
				}else{
					resp.setContentType("application/sparql-results+xml");
					r2r.executeSparql(query, ReturnType.XML, resp.getOutputStream());
					
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
		log.debug("Answered Query");
		
		
	}
	
	
	@Override
	public void destroy() {
		super.destroy();
	}

}
