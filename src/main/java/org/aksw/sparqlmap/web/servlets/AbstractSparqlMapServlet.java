package org.aksw.sparqlmap.web.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.aksw.sparqlmap.SparqlMap;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

public abstract class AbstractSparqlMapServlet extends HttpServlet{
	
	public SparqlMap sparqlmap;
	public WebApplicationContext ctxt;
	
	@Override
	protected final void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		prepareDoExecute(req, resp);
	}
	
	@Override
	protected final void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		prepareDoExecute(req, resp);
	}
	
	
	private void prepareDoExecute(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		if(ctxt == null ){
			 ctxt = WebApplicationContextUtils.getWebApplicationContext(this.getServletContext());

		}
		
		if(sparqlmap==null){
			 sparqlmap  = (SparqlMap) ctxt.getBean("sparqlMap");
			 
		}
		
		doExecute(req, resp);
	}
	
	public abstract void doExecute(HttpServletRequest req, HttpServletResponse resp);
	
	
	

}
