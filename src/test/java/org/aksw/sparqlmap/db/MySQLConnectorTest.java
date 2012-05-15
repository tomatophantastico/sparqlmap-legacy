package org.aksw.sparqlmap.db;

import static org.junit.Assert.*;

import java.io.InputStreamReader;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.aksw.sparqlmap.BaseTest;
import org.aksw.sparqlmap.config.syntax.MappingConfiguration;
import org.aksw.sparqlmap.config.syntax.R2RConfiguration;
import org.aksw.sparqlmap.config.syntax.SimpleConfigParser;
import org.junit.Before;
import org.junit.Test;

public class MySQLConnectorTest {

	private MySQLConnector mysqlconnector;

	@Before
	public void setUp() throws Exception {
		
		SimpleConfigParser parser = new SimpleConfigParser();

		R2RConfiguration config = parser.parse(new InputStreamReader(
				ClassLoader.getSystemResourceAsStream("bsbm.r2rml")));

	

		mysqlconnector = new MySQLConnector(config.getDbConn());
	}

	@Test
	public void test() {
		Table personTable = new Table(null, "person");
		List<SelectExpressionItem> seis = mysqlconnector.getSelectItemsForTable(personTable);
		Map<String,Integer> seiType = mysqlconnector.getDataTypeForTable(personTable);
		assertTrue(seis.size()==seiType.size());
		assertTrue(((Column)(seis.get(0).getExpression())).getColumnName().equals("nr"));
		assertTrue(seiType.get(0).equals(Types.INTEGER));
		assertTrue(((Column)(seis.get(1).getExpression())).getColumnName().equals("name"));
		assertTrue(seiType.get(1).equals(Types.VARCHAR));
		assertTrue(((Column)(seis.get(2).getExpression())).getColumnName().equals("mbox_sha1sum"));
		assertTrue(seiType.get(2).equals(Types.CHAR));
		assertTrue(((Column)(seis.get(3).getExpression())).getColumnName().equals("country"));
		assertTrue(seiType.get(3).equals(Types.CHAR));
		assertTrue(((Column)(seis.get(4).getExpression())).getColumnName().equals("publisher"));
		assertTrue(seiType.get(4).equals(Types.INTEGER));
		assertTrue(((Column)(seis.get(5).getExpression())).getColumnName().equals("publishDate"));
		assertTrue(seiType.get(5).equals(Types.DATE));
		
	}

}
