package org.aksw.sparqlmap.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;


public interface IDBAccess {

	public abstract SQLResultSetWrapper executeSQL(String sql)
			throws SQLException;

	public abstract List<SelectExpressionItem> getSelectItemsForView(
			Statement view);

	public abstract List<SelectExpressionItem> getSelectItemsForTable(
			Table table);

	public abstract Map<String, Integer> getDataTypeForView(
			Statement viewStatement);

	public abstract Map<String, Integer> getDataTypeForTable(Table table);

	public abstract void close();

	/**
	 * checks, wheter a given from item is valid or not.
	 * If null is returned, everything went well, otherwise there is the error message.
	 * @param fromItem
	 * @return
	 * @throws SQLException 
	 */

	public abstract void validateFromItem(FromItem fromItem)
			throws SQLException;

	/**used to determine the column name, as the db sees it on the interpreted from item.
	 * 
	 * used for example, if the sql contains an unsecaped alias for a column
	 * 
	 * @param fromItem
	 * @param unescapedColname
	 * @return
	 * @throws SQLException
	 */
	public abstract String getColumnName(FromItem fromItem,
			String unescapedColname);

	public abstract Integer getDataType(String alias, String colname);

	public abstract Integer getDataType(FromItem fromItem, String colname);

	public abstract ExpressionDeParser getExpressionDeParser(
			StringBuilder fromItemSb);

	public abstract Connection getConnection() throws SQLException;

	public abstract SelectDeParser getSelectDeParser(StringBuilder sb);

	public abstract ExpressionDeParser getExpressionDeParser(
			SelectDeParser selectDeParser, StringBuilder out);
	
	public abstract Integer getPrecision(String alias, String colname);



}