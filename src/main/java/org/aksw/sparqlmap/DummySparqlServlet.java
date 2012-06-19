package org.aksw.sparqlmap;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;

import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;

public class DummySparqlServlet extends HttpServlet{
	
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SPARQLServlet.class);

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
	
	public void doExecute(HttpServletRequest req, HttpServletResponse resp){
		String query = req.getParameter("query");
		QueryFactory.create(query);
		log.info("Anwering dummy");
		
		String answer = "<?xml version=\"1.0\"?>  <?xml-stylesheet type=\"text/xsl\" href=\"/xml-to-html.xsl\"?>  <sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">   <head>     <variable name=\"fake\"/>     <variable name=\"empty\"/>    </head>   <results>   </results> </sparql>";
		
			try {
				resp.getWriter().append(answer);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.error("Error:",e);
			}
		

	}
		
}
