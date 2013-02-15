package org.aksw.sparqlmap.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PreDestroy;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.deparser.AnsiQuoteExpressionDeParser;
import net.sf.jsqlparser.util.deparser.AnsiQuoteSelectDeparser;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLValidationException;
import org.aksw.sparqlmap.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.mapper.translate.ImplementationException;
import org.springframework.beans.factory.annotation.Autowired;


public class DBAccess {
	
	@Autowired
	private DataTypeHelper dataTypeHelper;
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DBAccess.class);
	
	public static String POSTGRES = "postgresql";
	public static String MYSQL = "mysql";
	public static String HSQLDB = "hsqldb"; 
	public DBAccess(Connector dbConnector, String dbname) {
		super();
		this.dbConnector = dbConnector;
		this.dbname = dbname;
	}

	private Connector dbConnector;
	private String dbname;

	

	public SQLResultSetWrapper executeSQL(String sql) throws SQLException{
		
		Connection connect  = dbConnector.getConnection();
		java.sql.Statement stmt = connect.createStatement();
		
		log.info("Executing translated Query: " +  sql);
		
		
		SQLResultSetWrapper wrap;
		try {
			wrap = new SQLResultSetWrapper(stmt.executeQuery(sql), connect,
					dataTypeHelper);
		} catch (SQLException e) {
			log.error("Error executing Query: " + sql);
			throw new SQLException(e);
		}
		
		return  wrap;
	}






	/* (non-Javadoc)
	 * @see org.aksw.sparqlmap.config.syntax.IDBAccess#getSelectItemsForTable(net.sf.jsqlparser.schema.Table)
	 */
	
	public List<SelectExpressionItem> getSelectItemsForTable(Table table) {
		return dbConnector.getSelectItemsForTable(table);
	}







	/* (non-Javadoc)
	 * @see org.aksw.sparqlmap.config.syntax.IDBAccess#getDataTypeForTable(net.sf.jsqlparser.schema.Table)
	 */
	
	public Map<String,Integer> getDataTypeForTable(Table table) {
		return dbConnector.getDataTypeForTable(table);
	}


	@PreDestroy
	public void close() {
		log.info("Closing the connections");
		dbConnector.close();
		
	}

	public void validateFromItem(FromItem fromItem) throws SQLException {
		String query = "SELECT * FROM " + fromItemToString(fromItem) +  "  LIMIT 1";
		
		   Connection conn = dbConnector.getConnection();
			ResultSet rs =  conn.createStatement().executeQuery(query);
			rs.close();
			conn.close();	
	}
	
	

	public String getColumnName(FromItem fromItem, String unescapedColname) {
		String query= null;
		   try {
			Connection conn = dbConnector.getConnection();
			query = "SELECT " +  conn.getMetaData().getIdentifierQuoteString()+unescapedColname +conn.getMetaData().getIdentifierQuoteString()+ " FROM " + fromItemToString(fromItem) +  "  LIMIT 1";

				ResultSet rs =  conn.createStatement().executeQuery(query);
				String name = rs.getMetaData().getColumnName(1);
				rs.close();
				conn.close();
				
				return name;
		} catch (SQLException e) {
			log.error("Error validating the column name, using the query: " + query + ".\n Does this column exist?");
			throw new R2RMLValidationException("Column name in virtual table mismatching definition in term map.",e);
		}
	}
	
	public Map<String,Map<String,Integer>> alias_col2datatype = new HashMap<String, Map<String,Integer>>();
	public Map<String,Map<String,Integer>> alias_col2precision = new HashMap<String, Map<String,Integer>>();
	
	
	/* (non-Javadoc)
	 * @see org.aksw.sparqlmap.config.syntax.IDBAccess#getDataType(java.lang.String, java.lang.String)
	 */
	
	public Integer getDataType(String alias, String colname) {
		if(alias_col2datatype.containsKey(alias)&&
				alias_col2datatype.get(alias).containsKey(colname)){
			return alias_col2datatype.get(alias).get(colname);
		}else{
			//throw new ImplementationException("Queried datatype for unregistered col:"  +alias + "." + colname);
			return null;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see org.aksw.sparqlmap.config.syntax.IDBAccess#getDataType(net.sf.jsqlparser.statement.select.FromItem, java.lang.String)
	 */
	
	public Integer getDataType(FromItem fromItem, String colname) {
		if(alias_col2datatype.containsKey(fromItem.getAlias())&&
				alias_col2datatype.get(fromItem.getAlias()).containsKey(colname)){
			return alias_col2datatype.get(fromItem.getAlias()).get(colname);
		}
		
		String query = "SELECT \"" +colname+ "\" FROM " + fromItemToString(fromItem) + " LIMIT 1";
		
		try {
			Connection conn = dbConnector.getConnection();
			java.sql.ResultSet rs = conn.createStatement().executeQuery(query);
			Integer resInteger = rs.getMetaData().getColumnType(1);
			Integer precision = rs.getMetaData().getPrecision(1);
			rs.close();
			conn.close();
			
			if(!alias_col2datatype.containsKey(fromItem.getAlias())){
				alias_col2datatype.put(fromItem.getAlias(), new HashMap<String, Integer>());
				alias_col2precision.put(fromItem.getAlias(), new HashMap<String, Integer>());
			}
			alias_col2datatype.get(fromItem.getAlias()).put(colname, resInteger);
			alias_col2precision.get(fromItem.getAlias()).put(colname, precision);
			
			return  resInteger;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error("Using the query: " + query);
			log.error("Querying for the datatype of " + colname  + ", from " + fromItemToString(fromItem) + " the following error was thrown: ",e);	
			throw new R2RMLValidationException("Querying for the datatype of " + colname  + ", from " + fromItemToString(fromItem) + " the following error was thrown: ", e);
		}
	}
	
	private String fromItemToString(FromItem fromItem){
		
		final StringBuilder fromItemSb = new StringBuilder();
		final SelectDeParser sdp  = getSelectDeParser(fromItemSb);
		
		sdp.setBuffer(fromItemSb);
		
		fromItem.accept(new FromItemVisitor() {
			
			
			public void visit(SubJoin subjoin) {
				throw new ImplementationException("Not implemented");
				
			}
			
			
			public void visit(SubSelect subSelect) {
				fromItemSb.append("( ");
				subSelect.getSelectBody().accept(sdp);

				fromItemSb.append(")  ");
				fromItemSb.append(" AS test ");
				
			}
			
			
			public void visit(Table tableName) {
				fromItemSb.append("");
				tableName.accept(sdp);
				fromItemSb.append("");
				//selectString.append(tableName.getName());
				
			}
		});
		
		return fromItemSb.toString();
		
	}
	
	
	
	
	/* (non-Javadoc)
	 * @see org.aksw.sparqlmap.config.syntax.IDBAccess#getExpressionDeParser(java.lang.StringBuffer)
	 */
	
	public ExpressionDeParser getExpressionDeParser(StringBuilder fromItemSb) {
		return getExpressionDeParser(getSelectDeParser(fromItemSb), fromItemSb);
		
	}
	
	
	/* (non-Javadoc)
	 * @see org.aksw.sparqlmap.config.syntax.IDBAccess#getConenction()
	 */
	
	public Connection getConnection() throws SQLException {
		return this.dbConnector.getConnection();

	}
	
	
	/* (non-Javadoc)
	 * @see org.aksw.sparqlmap.config.syntax.IDBAccess#getSelectDeParser(java.lang.StringBuffer)
	 */
	
	public SelectDeParser getSelectDeParser(StringBuilder sb) {
//		if(dbname.equals(MYSQL)){
//			
//			ExpressionDeParser expressionDeParser = new ExpressionDeParser();
//			SelectDeParser selectDeParser = new SelectDeParser();
//			expressionDeParser.setBuffer(sb);
//			selectDeParser.setBuffer(sb);
//			expressionDeParser.setSelectVisitor(selectDeParser);
//			selectDeParser.setExpressionVisitor(expressionDeParser);
//			
//			
//			return selectDeParser;
//		} else if(dbname.equals(POSTGRES )|| dbname.equals(HSQLDB)){
			AnsiQuoteSelectDeparser selectDeParser = new AnsiQuoteSelectDeparser();
			AnsiQuoteExpressionDeParser expressionDeParser = new AnsiQuoteExpressionDeParser(selectDeParser,sb);
			selectDeParser.setBuffer(sb);
			selectDeParser.setExpressionVisitor(expressionDeParser);
			return selectDeParser;
			
//		}
//		log.warn("Selected default deparser");
//		return new SelectDeParser();
	}
	
	
	/* (non-Javadoc)
	 * @see org.aksw.sparqlmap.config.syntax.IDBAccess#getExpressionDeParser(net.sf.jsqlparser.util.deparser.SelectDeParser, java.lang.StringBuffer)
	 */
	
	public ExpressionDeParser getExpressionDeParser(SelectDeParser selectDeParser, StringBuilder out) {
		return new AnsiQuoteExpressionDeParser(selectDeParser, out);
//		if(dbname.equals(MYSQL)){
//			return new ExpressionDeParser(selectDeParser, out);
//		} else if(dbname.equals(POSTGRES)||dbname.equals(HSQLDB)){
//			
//			
//		}
//		log.warn("Selected default expresseiondeparser");
//		return new ExpressionDeParser(selectDeParser,out);
	}



	
	public Integer getPrecision(String alias, String colname) {
		
		return alias_col2precision.get(alias).get(colname);
	}




}
