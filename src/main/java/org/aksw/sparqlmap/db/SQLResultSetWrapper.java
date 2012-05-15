package org.aksw.sparqlmap.db;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ColumnHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.SBlockNodeMapping;
import org.apache.commons.collections15.multimap.MultiHashMap;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.ResultBinding;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingHashMap;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;

public class SQLResultSetWrapper implements com.hp.hpl.jena.query.ResultSet {
	
	DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz");

	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(SQLResultSetWrapper.class);

	private ResultSet rs;

	private List<String> vars = new ArrayList<String>();
	private List<String> colNames = new ArrayList<String>();

	private Connection conn;

	private boolean didNext = false;

	private boolean hasNext  = false;

	private DataTypeHelper dth;

	public SQLResultSetWrapper(ResultSet rs, Connection conn, DataTypeHelper dth)
			throws SQLException {
		this.conn = conn;
		this.rs = rs;
		this.dth = dth;
		initVars();

	}
	
	private Multimap< String, String> var2ResourceCols = TreeMultimap.create();

	/**
	 * for finding all the columns, we rely on that there is a type col for each
	 * line
	 * 
	 * @throws SQLException
	 */
	private void initVars() throws SQLException {

		for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
			String colname = rs.getMetaData().getColumnName(i);
			colNames.add(colname);
			
			if (colname.endsWith(ColumnHelper.TYPE_COL)) {
				vars.add(colname.substring(0, colname.length()
						- ColumnHelper.TYPE_COL.length()));
			}
			
			if(colname.contains(ColumnHelper.RESOURCE_COL_SEGMENT)){
				//we extract the var name
				String var = ColumnHelper.colnameBelongsToVar(colname);
				var2ResourceCols.put(var,colname);
			}

		}
	}

	@Override
	public void remove() {
		throw new ImplementationException("thou shalt not remove!");

	}

	@Override
	public boolean hasNext() {

		if (!didNext) {
			try {
				hasNext = rs.next();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.error("Error:", e);
				hasNext = false;
			}
			didNext = true;
		}

		if (!hasNext) {
			close();
		}
		return hasNext;

	}

	@Override
	public QuerySolution next() {
		return nextSolution();
	}


	@Override
	public QuerySolution nextSolution() {
		return new ResultBinding(null, nextBinding());
	}


	@Override
	public Binding nextBinding() {
		BindingMap binding = null;
		try {
			if (!didNext) {
				rs.next();
			}
			didNext = false;

			binding = new BindingHashMap();

			for (String var : vars) {
				Node node;
				// create the binding here
				// first check for type
				int type = rs.getInt(var + ColumnHelper.TYPE_COL);
				if (type == ColumnHelper.COL_TYPE_RESOURCE) {
					StringBuffer uri = new StringBuffer();
					for(String colname : this.var2ResourceCols.get(var)){
						String segment =rs.getString(colname);
						if(segment !=null){
							uri.append(segment);
						}
					}
				
					if(uri.length()==0){
						node = null; 
					}else{
						node = Node.createURI(uri.toString());
					}
					
				} else if (type == ColumnHelper.COL_TYPE_LITERAL) {
					// node =
					// Node.createLiteral(rs.getString(var+ColumnHelper.LITERAL_COL_STRING));

					// determine the data type
					int sqldatatype = rs.getInt(var
							+ ColumnHelper.LIT_TYPE_COL);
						
					String literalVal = null;
					if(dth.getCastTypeString(sqldatatype) == dth.getStringCastType()&&
							rs.getString(var + ColumnHelper.LITERAL_COL_STRING)!=null){
						literalVal = rs.getString(var + ColumnHelper.LITERAL_COL_STRING);
					}else if(dth.getCastTypeString(sqldatatype) == dth.getDateCastType()&&
							rs.getDate(var+ColumnHelper.LITERAL_COL_DATE)!=null){
						literalVal = formatter.format(rs.getDate(var+ColumnHelper.LITERAL_COL_DATE));
					}else if(dth.getCastTypeString(sqldatatype) == dth.getNumericCastType() &&
							rs.getBigDecimal(var + ColumnHelper.LITERAL_COL_NUM) != null){
						literalVal = rs.getBigDecimal(var + ColumnHelper.LITERAL_COL_NUM).toString();
					}else if(dth.getCastTypeString(sqldatatype) == dth.getBooleanCastType()){
						throw new ImplementationException("No Boolean support here.");
					}
					
					if(literalVal !=null){
						

					node = Node
							.createLiteral(
									literalVal,
									null,
									DataTypeHelper.getRDFDataType(sqldatatype));
					}else{
						node = null; 
					}

				} else {
					node = null;
				}
				binding.add(Var.alloc(var), node);

			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error("Error:", e);
		}
		return binding;
	}

	@Override
	public int getRowNumber() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<String> getResultVars() {
		return vars;
	}

	@Override
	public Model getResourceModel() {
		log.warn("getREsourceModel not implemented");
		return null;
	}

	public ResultSet getRs() {
		return rs;
	}

	public void close() {
		try {
			if (rs.isClosed() == false) {
				rs.close();
				conn.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error("Error:", e);
		}
	}

}
