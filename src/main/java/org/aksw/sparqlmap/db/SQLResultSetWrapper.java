package org.aksw.sparqlmap.db;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.aksw.sparqlmap.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
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
	public void initVars() throws SQLException {

		for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
			String colname = rs.getMetaData().getColumnName(i);
			colNames.add(colname);
			
			if (colname.endsWith(ColumnHelper.COL_NAME_RDFTYPE)) {
				vars.add(colname.substring(0, colname.length()
						- ColumnHelper.COL_NAME_RDFTYPE.length()));
			}
			
			if(colname.contains(ColumnHelper.COL_NAME_RESOURCE_COL_SEGMENT)){
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
				int type = rs.getInt(var + ColumnHelper.COL_NAME_RDFTYPE);
				if (type == ColumnHelper.COL_VAL_TYPE_RESOURCE) {
					StringBuffer uri = new StringBuffer();
					int i = 0;
					for(String colname : this.var2ResourceCols.get(var)){
						if(i++%2==0){
							//fix string
							String segment =rs.getString(colname);
							if(segment !=null){
								uri.append(segment);
							}
						}else{
							//column derived valued
							String segment =rs.getString(colname);
							if(segment !=null){
								try {
									
									
									uri.append(URLEncoder.encode(segment, "UTF-8").replaceAll("\\+", "%20"));
								} catch (UnsupportedEncodingException e) {
									// TODO Auto-generated catch block
									log.error("Error:",e);
								}
							}
						}
						
					}
				
					if(uri.length()==0){
						node = null; 
					}else{
						node = Node.createURI(uri.toString());
					}
					
				} else if (type == ColumnHelper.COL_VAL_TYPE_LITERAL) {
					
					String litType = rs.getString(var + ColumnHelper.COL_NAME_LITERAL_TYPE);
					RDFDatatype dt = null;
					if(litType!=null&&!litType.isEmpty()){
						dt = new BaseDatatype(litType);
					}
					
					String lang =  rs.getString(var + ColumnHelper.COL_NAME_LITERAL_LANG);
					
					
					
					// node =
					// Node.createLiteral(rs.getString(var+ColumnHelper.LITERAL_COL_STRING));

					// determine the data type
					int sqldatatype = rs.getInt(var
							+ ColumnHelper.COL_NAME_SQL_TYPE);
					
	
					String literalValue;
					
					if(XSDDatatype.XSDdecimal.getURI().equals(litType)||XSDDatatype.XSDinteger.getURI().equals(litType) || XSDDatatype.XSDdouble.getURI().equals(litType)){
						literalValue = rs.getBigDecimal(var + ColumnHelper.COL_NAME_LITERAL_NUMERIC).toString();
					}else if(XSDDatatype.XSDstring.getURI().equals( litType)|| litType ==null){
						literalValue = rs.getString(var + ColumnHelper.COL_NAME_LITERAL_STRING);
					}else if(XSDDatatype.XSDdateTime.getURI().equals(litType)){
						
											
						literalValue = ISODateTimeFormat.basicDateTime().print(rs.getDate(var+ColumnHelper.COL_NAME_LITERAL_DATE).getTime());
						
						
						literalValue = formatter.format(rs.getDate(var+ColumnHelper.COL_NAME_LITERAL_DATE));
					}else if(XSDDatatype.XSDdate.getURI().equals(litType) ){
						literalValue = ISODateTimeFormat.basicDate().print(rs.getDate(var+ColumnHelper.COL_NAME_LITERAL_DATE).getTime());
						
					}else if( XSDDatatype.XSDtime.getURI().equals(litType)){
						literalValue = ISODateTimeFormat.basicTime().print(rs.getDate(var+ColumnHelper.COL_NAME_LITERAL_DATE).getTime());
					}else if(XSDDatatype.XSDboolean.getURI().equals(litType)){
						literalValue = Boolean.toString(rs.getBoolean(var+ColumnHelper.COL_NAME_LITERAL_BOOL));
					}else if(XSDDatatype.XSDhexBinary.getURI().equals(litType)){
						literalValue = XSDDatatype.XSDhexBinary.unparse(rs.getBytes(ColumnHelper.COL_NAME_LITERAL_BINARY));
					}else{
						throw new ImplementationException("Cannot map into result set");
					}
					
					
						
					
					
					
					if(literalValue !=null){
						

					node = Node
							.createLiteral(
									literalValue,
									lang, dt
									);
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
