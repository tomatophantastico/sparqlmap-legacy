package org.aksw.sparqlmap.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public interface Connector {


	public Connection getConnection() throws SQLException;
	
	
	public List<SelectExpressionItem> getSelectItemsForView(Statement view);



	public List<SelectExpressionItem> getSelectItemsForTable(Table table);
	
	


	public Map<String,Integer> getDataTypeForView(Statement viewStatement);



	public Map<String,Integer> getDataTypeForTable(Table table);


	public void close();


}