package org.aksw.sparqlmap.r2rmltestcases;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.aksw.sparqlmap.SparqlMap;
import org.aksw.sparqlmap.SparqlMap.ReturnType;
import org.aksw.sparqlmap.db.SQLResultSetWrapper;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Multimap;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;

public abstract class BSBMBaseTest {

	
	public abstract SparqlMap getSparqlMap();
	
	public void validate(String execAsText, Set<String> resString) {
		for (String string : resString) {
			Assert.assertTrue("Result does not contain " + string + ", looks like this: "+ execAsText ,execAsText.contains(string));
		}
	}

	public SQLResultSetWrapper exec(String query) throws SQLException{
		SQLResultSetWrapper rsw = getSparqlMap().executeSparql(QueryFactory.create(query));
		
		return rsw;

	}
	/**
	 * executes a query and returns the result as text
	 * @param query
	 * @return
	 * @throws SQLException
	 */
	public String execAsText(String query) throws SQLException{
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		getSparqlMap().executeSparql(query, ReturnType.XML, out);
		SQLResultSetWrapper rsw = getSparqlMap().executeSparql(QueryFactory.create(query));
		
		return out.toString();

	}
	
	
	public void validate(SQLResultSetWrapper rsw, Multimap<String, String> expectedResults, int benchmarkcount){
	
		int actualcount = 0;
		while(rsw.hasNext()){
			actualcount++;
			QuerySolution sol =rsw.next();
			for(String var: new HashSet<String>(expectedResults.keySet())){
				String varsol = sol.get(var).toString();
				expectedResults.remove(var, varsol);
			}
		}
		
		Assert.assertTrue(" expected Results not empty, still contain: " +expectedResults,expectedResults.isEmpty());
		Assert.assertTrue("Resultset size does not matc, should be " +benchmarkcount + " but is actualy " + actualcount,actualcount == benchmarkcount);
		
		
		
		
	}

	@Test
	public void testQ1() throws SQLException{
		validate(exec(BSBM100k.q1),BSBM100k.q1Res(),BSBM100k.q1count);
	}
	
	@Test
	public void testQ2() throws SQLException{
		validate(exec(BSBM100k.q2),BSBM100k.q2Res(),BSBM100k.q2count);
	}
	
	@Test
	public void testQ3() throws SQLException{
		validate(exec(BSBM100k.q3),BSBM100k.q3Res(),BSBM100k.q3count);
	}
	@Test
	public void testQ4() throws SQLException{
		validate(exec(BSBM100k.q4),BSBM100k.q4Res(),BSBM100k.q4count);
	}
	@Test
	public void testQ5() throws SQLException{
		validate(exec(BSBM100k.q5),BSBM100k.q5Res(),BSBM100k.q5count);
	}
	@Test
	public void testQ7() throws SQLException{
		validate(exec(BSBM100k.q7),BSBM100k.q7Res(),BSBM100k.q7count);
	}
	@Test
	public void testQ8() throws SQLException{
		validate(exec(BSBM100k.q8),BSBM100k.q8Res(),BSBM100k.q8count);
	}
	@Test
	public void testQ9() throws SQLException{
		validate(execAsText(BSBM100k.q9),BSBM100k.q9Res());
	}
	@Test
	public void testQ10() throws SQLException{
		validate(exec(BSBM100k.q10),BSBM100k.q10Res(),BSBM100k.q10count);
	}
	@Test
	public void testQ11() throws SQLException{
		validate(exec(BSBM100k.q11),BSBM100k.q11Res(),BSBM100k.q11count);
	}
	@Test
	public void testQ12() throws SQLException{
		validate(execAsText(BSBM100k.q12),BSBM100k.q12Res());
	}

}
