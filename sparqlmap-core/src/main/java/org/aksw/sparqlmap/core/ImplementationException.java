package org.aksw.sparqlmap.core;

/**
 * This Exception is thrown when not implemented modules of SparqlMap are called. 
 * @author joerg
 *
 */
public class ImplementationException extends RuntimeException {
	public ImplementationException(String msg) {
		super(msg);
	}

}
