package org.aksw.sparqlmap;

import java.io.StringReader;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import org.junit.Test;
public class SQLParserTest {
	
	
	
	@Test
	public void parserMultiTableTest() throws JSQLParserException{
		String sql  = "SELECT * from USER u, manager m , test t where u.id = '1' AND u.name = '3' AND m.id = '2'";
		Statement sqlStmt = new CCJSqlParserManager().parse(new StringReader(sql));
		StringBuffer stringBuffer = new StringBuffer();
		StatementDeParser deparser = new StatementDeParser(stringBuffer);
		
		sqlStmt.accept(deparser);
		System.out.println(stringBuffer);

	}

	
	@Test
	public void parserTest() throws JSQLParserException{
		String sql  = "SELECT *  	FROM   employee	       INNER JOIN department	          ON employee.DepartmentID = department.DepartmentID where employee.name like '%asfd%'";
		Statement sqlStmt = new CCJSqlParserManager().parse(new StringReader(sql));
		StringBuffer stringBuffer = new StringBuffer();
		StatementDeParser deparser = new StatementDeParser(stringBuffer);
		
		sqlStmt.accept(deparser);
		System.out.println(stringBuffer);
		
		
	}
	
	@Test
	public void subselectParserTEst() throws JSQLParserException{
		String sql  = "SELECT  p.label as label, subselect.title from product as p inner join (select product, title from review) as subselect where subselect.product = p.nr limit 10";
		Statement sqlStmt = new CCJSqlParserManager().parse(new StringReader(sql));
		StringBuffer stringBuffer = new StringBuffer();
		StatementDeParser deparser = new StatementDeParser(stringBuffer);
		
		sqlStmt.accept(deparser);
		System.out.println(stringBuffer);
		
		
	}
	@Test
	public void castTest() throws JSQLParserException{
		String sql  = "SELECT  CAST(nr AS CHAR(23)) as test, label from product";
		Statement sqlStmt = new CCJSqlParserManager().parse(new StringReader(sql));
		StringBuffer stringBuffer = new StringBuffer();
		StatementDeParser deparser = new StatementDeParser(stringBuffer);
		
		sqlStmt.accept(deparser);
		System.out.println(stringBuffer);
		
		
	}
	
	@Test
	public void projectTest() throws JSQLParserException{
		String sql  = "select * from test ORDER BY no1, no2 limit 10 offset 20 ";
		Statement sqlStmt = new CCJSqlParserManager().parse(new StringReader(sql));
		StringBuffer stringBuffer = new StringBuffer();
		StatementDeParser deparser = new StatementDeParser(stringBuffer);
		
		sqlStmt.accept(deparser);
		System.out.println(stringBuffer);
	}
	
	
	
	@Test
	public void subStringTest() throws JSQLParserException{
		String sql  = "SELECT  'test' as test, label from product";
		Statement sqlStmt = new CCJSqlParserManager().parse(new StringReader(sql));
		StringBuffer stringBuffer = new StringBuffer();
		StatementDeParser deparser = new StatementDeParser(stringBuffer);
		
		sqlStmt.accept(deparser);
		System.out.println(stringBuffer);
		
		
	}
	@Test
	public void notNullTest() throws JSQLParserException{
		String sql  = "SELECT  'test' as test, label from product where product.nr is not null";
		Statement sqlStmt = new CCJSqlParserManager().parse(new StringReader(sql));
		StringBuffer stringBuffer = new StringBuffer();
		StatementDeParser deparser = new StatementDeParser(stringBuffer);
		
		sqlStmt.accept(deparser);
		System.out.println(stringBuffer);
		
		
	}
	@Test
	public void functionInOrderByTest() throws JSQLParserException{
		String sql  = "SELECT  * from product where product.nr is not null order by sum(product.nr,product.whatever) ASC";
		Statement sqlStmt = new CCJSqlParserManager().parse(new StringReader(sql));
		StringBuffer stringBuffer = new StringBuffer();
		StatementDeParser deparser = new StatementDeParser(stringBuffer);
		
		sqlStmt.accept(deparser);
		System.out.println(stringBuffer);
		
		
	}
	


}
