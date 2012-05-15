package org.aksw.sparqlmap.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import org.aksw.sparqlmap.config.syntax.DBConnectionConfiguration;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;

import com.hp.hpl.jena.vocabulary.DB;

public class SQLAccessFacade  {
	
	Connector conn = null;
	private DataTypeHelper dth;
	
	
	
	public SQLAccessFacade(DBConnectionConfiguration dbConn) {
		this.dth =  dbConn.getDataTypeHelper();
		String dbname = dbConn.getDbConnString().split(":")[1];
		if(dbname.equals("mysql")){
			conn = new MySQLConnector(dbConn);
		}else if(dbname.equals("postgresql")){
			conn = new PostgesqlConnector(dbConn);
		}
		
		
	}


	
	public SQLResultSetWrapper executeSQL(String sql) throws SQLException{
		
		Connection connect  = conn.getConnection();
		java.sql.Statement stmt = connect.createStatement();
		
		
		SQLResultSetWrapper wrap = new SQLResultSetWrapper(stmt.executeQuery(sql), connect, dth);
		
		return  wrap;
	}



	public List<SelectExpressionItem> getSelectItemsForView(Statement view) {	
		return conn.getSelectItemsForView(view);
	}



	public List<SelectExpressionItem> getSelectItemsForTable(Table table) {
		return conn.getSelectItemsForTable(table);
	}



	public Map<String,Integer> getDataTypeForView(Statement viewStatement) {
		return conn.getDataTypeForView( viewStatement);
	}



	public Map<String,Integer> getDataTypeForTable(Table table) {
		return conn.getDataTypeForTable(table);
	}



	public void close() {
		conn.close();
		
	}



}
