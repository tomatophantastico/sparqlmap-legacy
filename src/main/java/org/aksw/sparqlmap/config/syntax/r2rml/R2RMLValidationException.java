package org.aksw.sparqlmap.config.syntax.r2rml;

import java.sql.SQLException;



public class R2RMLValidationException extends RuntimeException{


	public R2RMLValidationException(String msg) {
		super(msg);
	}
	
	public R2RMLValidationException(String msg,Throwable e) {
		super(msg,e);
	}

	
	
	

}
