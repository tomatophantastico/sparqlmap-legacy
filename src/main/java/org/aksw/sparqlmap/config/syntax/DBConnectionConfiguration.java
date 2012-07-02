package org.aksw.sparqlmap.config.syntax;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLValidationException;
import org.aksw.sparqlmap.db.Connector;
import org.aksw.sparqlmap.db.MySQLConnector;
import org.aksw.sparqlmap.db.PostgesqlConnector;
import org.aksw.sparqlmap.db.SQLResultSetWrapper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

import com.hp.hpl.jena.query.ResultSet;


public class DBConnectionConfiguration {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DBConnectionConfiguration.class);
	
	public static String POSTGRES = "postgresql";
	public static String MYSQL = "mysql";
	
	private DataTypeHelper dataTypeHelper;
	
		
	private String dbUrl;
	private String username;
	private String password;
	private int poolminconnections;
	private int poolmaxconnections;
	
	private Connector dbConnector;

	public DBConnectionConfiguration(File databaseConfFileName) throws FileNotFoundException, IOException {
		
		Properties props = new Properties();
		props.load(new FileInputStream(databaseConfFileName));
		
		
		this.dbUrl = props.getProperty("jdbc.url");
		this.username = props.getProperty("jdbc.username");
		this.password = props.getProperty("jdbc.password");
		this.poolminconnections = new Integer(props.getProperty("jdbc.poolminconnections"));
		this.poolmaxconnections = new Integer(props.getProperty("jdbc.poolmaxconnections"));

		
		String dbname =  getJdbcDBName();
		if(dbname.equals("mysql")){
			dataTypeHelper = new MysqlDataTypeHelper();
			dbConnector = new MySQLConnector(dbUrl, username, password, poolminconnections, poolmaxconnections);
		}else if(dbname.equals("postgresql")){
			dataTypeHelper = new PostgresDataTypeHandler();
			dbConnector = new PostgesqlConnector(dbUrl, username, password, poolminconnections, poolmaxconnections);
		}else{
			log.error("Unknown Database string " + dbname + " encountered");
		}
		
	}

	public String getDbConnString() {
		return dbUrl;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}
	
	public DataTypeHelper getDataTypeHelper() {
		return dataTypeHelper;
	}
	
	public String getJdbcDBName(){
		return dbUrl.split(":")[1];
	}

	public SelectDeParser getSelectDeParser(StringBuffer sb) {
		if(getJdbcDBName().equals(MYSQL)){
			
			ExpressionDeParser expressionDeParser = new ExpressionDeParser();
			SelectDeParser selectDeParser = new SelectDeParser();
			expressionDeParser.setBuffer(sb);
			selectDeParser.setBuffer(sb);
			expressionDeParser.setSelectVisitor(selectDeParser);
			selectDeParser.setExpressionVisitor(expressionDeParser);
			
			
			return selectDeParser;
		} else if(getJdbcDBName().equals(POSTGRES)){
			PostgresqlSelectDeparser selectDeParser = new PostgresqlSelectDeparser();
			PostgresqlExpressionDeParser expressionDeParser = new PostgresqlExpressionDeParser(selectDeParser,sb);
			selectDeParser.setBuffer(sb);
			selectDeParser.setExpressionVisitor(expressionDeParser);
			
			
			return selectDeParser;
			
		}
		log.warn("Selected default deparser");
		return new SelectDeParser();
	}
	public ExpressionDeParser getExpressionDeParser(SelectDeParser selectDeParser, StringBuffer out) {
		if(getJdbcDBName().equals(MYSQL)){
			return new ExpressionDeParser(selectDeParser, out);
		} else if(getJdbcDBName().equals(POSTGRES)){
			return new PostgresqlExpressionDeParser(selectDeParser, out);
			
		}
		log.warn("Selected default expresseiondeparser");
		return new ExpressionDeParser(selectDeParser,out);
	}
	
	
	
	
	public SQLResultSetWrapper executeSQL(String sql) throws SQLException{
		
		Connection connect  = dbConnector.getConnection();
		java.sql.Statement stmt = connect.createStatement();
		
		
		SQLResultSetWrapper wrap = new SQLResultSetWrapper(stmt.executeQuery(sql), connect, dataTypeHelper);
		
		return  wrap;
	}



	public List<SelectExpressionItem> getSelectItemsForView(Statement view) {	
		return dbConnector.getSelectItemsForView(view);
	}



	public List<SelectExpressionItem> getSelectItemsForTable(Table table) {
		return dbConnector.getSelectItemsForTable(table);
	}



	public Map<String,Integer> getDataTypeForView(Statement viewStatement) {
		return dbConnector.getDataTypeForView( viewStatement);
	}



	public Map<String,Integer> getDataTypeForTable(Table table) {
		return dbConnector.getDataTypeForTable(table);
	}



	public void close() {
		dbConnector.close();
		
	}


	/**
	 * checks, wheter a given from item is valid or not.
	 * If null is returned, everything went well, otherwise there is the error message.
	 * @param fromItem
	 * @return
	 */

	public String validateFromItem(FromItem fromItem) {
		StringBuffer fromItemSb = new StringBuffer();
		SelectDeParser sdp  = getSelectDeParser(fromItemSb);
		
		sdp.setBuffer(fromItemSb);
		
		fromItem.accept(sdp);
		
		
		
		String query = "SELECT * FROM " + fromItemSb.toString() + " LIMIT 1";
		
		try {
			executeSQL(query);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error("Error:",e);
			return e.getMessage();
		}
	
				
		return null;
	}

	public Integer getDataType(FromItem fromItem, String colname) {
		StringBuffer fromItemSb = new StringBuffer();
		getExpressionDeParser(fromItemSb);
		
		
		String query = "SELECT \"" +colname+"\" FROM " + fromItemSb.toString() + " LIMIT 1";
		
		try {
			java.sql.ResultSet rs = dbConnector.executeSQL(query);
			return rs.getMetaData().getColumnType(1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error("Querying for the datatype of " + colname  + ", from " + fromItemSb.toString() + " the following error was thrown: ",e);
			throw new R2RMLValidationException("\"Querying for the datatype of \" + colname  + \", from \" + fromItemSb.toString() + \" the following error was thrown: " + e.getMessage());
		}
	}
	public ExpressionDeParser getExpressionDeParser(StringBuffer fromItemSb) {
		return getExpressionDeParser(getSelectDeParser(fromItemSb), fromItemSb);
		
	}
	
	
	

}
