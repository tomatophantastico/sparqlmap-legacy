package org.aksw.sparqlmap.core.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultSetSQLToSPARQL {
	
	
	
	
	
	
	
	
	
	
	public static String convert(ResultSet rs) throws SQLException {
		StringBuffer sparqlResult = new StringBuffer();
		//create preambel
		
		
		sparqlResult.append("<?xml version=\"1.0\"?>\n"+
				"<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n");
		
		//add the headers
		sparqlResult.append("  <head>\n");
		for (int i = 0;i<rs.getMetaData().getColumnCount();i++) {
			sparqlResult.append("    <variable name=\""+ rs.getMetaData().getColumnLabel(i)+"\"/>\n");
		}
		sparqlResult.append("  </head>\n  <results>");
		
		//iterate through the results
		while(rs.next()){
			sparqlResult.append("<result>\n");
			for (int i = 1;i<=rs.getMetaData().getColumnCount();i++) {
				sparqlResult.append("    <binding name=\""+ rs.getMetaData().getColumnLabel(i)+"\"/>\n");
				String rsString = rs.getString(i);
				//TODO we need to know if it is REALLY a uri
				if(rsString.contains("://")){
					sparqlResult.append("    <uri>"+ rsString+"</uri>");
				}else{
					sparqlResult.append("    <literal>"+ rsString+"</literal>");
				}
				sparqlResult.append("    </binding>\n");

				
			}
			
			sparqlResult.append("<result>\n");
		}
		sparqlResult.append("  </head>\n  <results>");
		
		return sparqlResult.toString();
		
		
	}

}
