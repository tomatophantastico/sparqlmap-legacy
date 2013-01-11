package org.aksw.sparqlmap.config.syntax.r2rml;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

import org.aksw.sparqlmap.db.IDBAccess;
import org.aksw.sparqlmap.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.mapper.translate.FilterUtil;
import org.aksw.sparqlmap.mapper.translate.ImplementationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;


@Component
public class ColumnHelper {
	
	public static String R2R_COL_SUFFIX = "_R2R";
	public static String COL_NAME_RDFTYPE = R2R_COL_SUFFIX + "_1_TYPE";
	public static String COL_NAME_RES_LENGTH = R2R_COL_SUFFIX + "_2_RES_LENGTH";
	public static String COL_NAME_SQL_TYPE = R2R_COL_SUFFIX + "_3_SQL_TYPE";
	public static String COL_NAME_LITERAL_TYPE = R2R_COL_SUFFIX + "_4_LIT_TYPE";
	public static String COL_NAME_LITERAL_LANG = R2R_COL_SUFFIX + "_5_LIT_LAN";
	public static String COL_NAME_GRAPH = R2R_COL_SUFFIX + "_6_GRAPH";
	public static String COL_NAME_RESOURCE_COL_SEGMENT = R2R_COL_SUFFIX
			+ "_7_SEG";
	public static String COL_NAME_LITERAL_STRING = R2R_COL_SUFFIX
			+ "_8_LIT_STRING";
	public static String COL_NAME_LITERAL_NUMERIC = R2R_COL_SUFFIX
			+ "_9_LIT_NUM";
	public static String COL_NAME_LITERAL_DATE = R2R_COL_SUFFIX + "_10_LIT_DATE";
	public static String COL_NAME_LITERAL_BOOL = R2R_COL_SUFFIX
			+ "_11_LIT_BOOL";
	public static String COL_NAME_LITERAL_BINARY = R2R_COL_SUFFIX + "12_LIT_BINARY";
	public static String COL_NAME_ORDER_BY = R2R_COL_SUFFIX + "_13_OBY";

	public static String COL_NAME_INTERNAL = "BTF";

	public static Integer COL_VAL_TYPE_RESOURCE = 1;
	public static Integer COL_VAL_TYPE_LITERAL = 2;
	public static Integer COL_VAL_TYPE_BLANK = 3;

	public static Integer COL_VAL_SQL_TYPE_RESOURCE = -9999;
	public static Integer COL_VAL_SQL_TYPE_CONSTLIT = -9998;
	public static Integer COL_VAL_RES_LENGTH_LITERAL = 0;

	public static String colnameBelongsToVar(String colalias) {
		return colalias.substring(0, colalias.indexOf(R2R_COL_SUFFIX));
	}
	public static boolean isColnameResourceSegment(String colalias) {
		
		return colalias.substring(colalias.indexOf(R2R_COL_SUFFIX)+4).startsWith(COL_NAME_RESOURCE_COL_SEGMENT);
	}
	
	@Autowired
	IDBAccess dbaccess;
	
	@Autowired
	DataTypeHelper dth;

	public List<Expression> getBaseExpressions(Integer type,
			Integer resLength, Integer sqlType, DataTypeHelper dth,
			String datatype, String lang, Column langColumn, Expression graph) {
		List<Expression> baseExpressions = new ArrayList<Expression>();
		LongValue typeVal = new LongValue(Integer.toString(type));
		baseExpressions.add(dth.cast(typeVal, dth.getNumericCastType()));
		LongValue lengthVal = new LongValue(Integer.toString(resLength));
		baseExpressions
				.add(dth.cast(lengthVal, dth.getNumericCastType()));
		LongValue sqlTypeVal = new LongValue(Integer.toString(sqlType));
		baseExpressions.add(dth.cast(sqlTypeVal,
				dth.getNumericCastType()));
		if (datatype == null) {
			baseExpressions.add(dth.cast(
					new net.sf.jsqlparser.expression.NullValue(),
					dth.getStringCastType()));
		} else {
			baseExpressions.add(dth.cast(
					new StringValue("\""
							+ datatype + "\""), dth.getStringCastType()));
		}

		if (lang != null) {
			if(lang.length()!=2&&!lang.toLowerCase().equals(lang)){
				throw new R2RMLValidationException("language string must be a two letter code in lower cases");
			}
			baseExpressions.add(dth.cast(
					new StringValue("\"" + lang
							+ "\""), dth.getStringCastType()));
		} else if (langColumn != null) {
			baseExpressions.add(dth.cast(langColumn, dth.getStringCastType()));
		} else {
			baseExpressions.add(dth.cast(new NullValue(), dth.getStringCastType()));
		}
		if (graph == null) {
			baseExpressions.add(dth.cast(
					new net.sf.jsqlparser.expression.NullValue(),
					dth.getStringCastType()));
		} else {
			baseExpressions.add(graph);
		}

		return baseExpressions;
	}
	
	
	
	

	/**
	 * create the Expressions for a constant values term map
	 * 
	 * @param node
	 * @param dth
	 * @return
	 */
	public List<Expression> getExpression(RDFNode node,
			DataTypeHelper dth,Expression graph) {
		List<Expression> texprs = new ArrayList<Expression>();

		if (node.isURIResource()) {
			texprs.addAll(getBaseExpressions(COL_VAL_TYPE_RESOURCE, 1,
					COL_VAL_SQL_TYPE_RESOURCE, dth, null, null, null,graph));
			//texprs.add(dth.cast(new StringValue("\"\""), dth.getStringCastType()));
			texprs.add(asExpression(node.asResource().getURI(), dth));
		} else if (node.isLiteral()) {
			texprs.addAll(getBaseExpressions(COL_VAL_TYPE_LITERAL, 0,
					COL_VAL_SQL_TYPE_CONSTLIT, dth, node.asLiteral()
							.getDatatypeURI(), node.asLiteral().getLanguage(),
					null,graph));
			
			//get the cast type
			
			Literal nodeL = (Literal) node.asLiteral();

			texprs.add(dth.cast(new StringValue("\"" + nodeL.getLexicalForm() + "\""), dth.getCastTypeString(nodeL.getDatatype())));
		} else {
			throw new ImplementationException(
					"No support for constant blank nodes in SparqlMap");
		}

		return texprs;
	}

	

	/**
	 * Create the expressions for a term map, that is producing a literal, based
	 * on columns.
	 * 
	 * @param col
	 * @param sqlType
	 * @param datatype
	 * @param lang
	 * @param lanColumn
	 * @param dth
	 * @return
	 */
	public List<Expression> getExpression(Column col, Integer rdfType,
			Integer sqlType, String datatype, String lang, Column lanColumn,
			DataTypeHelper dth, Expression graph, String baseUri) {
		List<Expression> texprs = new ArrayList<Expression>();

		if (rdfType.equals(COL_VAL_TYPE_LITERAL)) {

			// if datatype not declared, we check, if we got a default
			// conversion
			if (datatype == null
					&& DataTypeHelper.getRDFDataType(sqlType) != null) {
				datatype = DataTypeHelper.getRDFDataType(sqlType).getURI();
			}

			texprs.addAll(getBaseExpressions(rdfType,
					COL_VAL_RES_LENGTH_LITERAL, sqlType, dth, datatype, lang,
					null,graph));
			RDFDatatype rdfdt = tm.getTypeByName(datatype); 
			if(datatype==null||rdfdt==null){
				texprs.add(dth.cast(col, dth.getCastTypeString(sqlType)));
			}else{
				texprs.add(dth.cast(col, dth.getCastTypeString(rdfdt)));
			}

		} else if (rdfType.equals(COL_VAL_TYPE_RESOURCE)||rdfType.equals(COL_VAL_TYPE_BLANK)) {
			texprs.addAll(getBaseExpressions(rdfType,
					1, COL_VAL_SQL_TYPE_RESOURCE, dth, datatype, lang,
					null,graph));
			//texprs.add(dth.cast(new StringValue("\"\""), dth.getStringCastType()));
			texprs.add(dth.cast(col, dth.getStringCastType()));

		} 
		return texprs;

	}
	
	TypeMapper tm = TypeMapper.getInstance();
	
	


	public  List<Expression> getExpression(String[] template,
			Integer rdfType, int sqlType, String datatype, String lang, Column lanColumn, 
			DataTypeHelper dth, FromItem fi,Expression graph,String baseUri) {
		List<Expression> texprs = new ArrayList<Expression>();
		
	
		
		List<String>  altSeq = Arrays.asList(template);
		
		
//		if(template.startsWith("{") && rdfType != COL_VAL_TYPE_LITERAL){
//			altSeq.add(0, "");
//		}
		
		
		if (rdfType.equals(COL_VAL_TYPE_LITERAL)) {
			// if datatype not declared, we check, if we got a default
			// conversion
			if (datatype == null
					&& DataTypeHelper.getRDFDataType(sqlType) != null) {
				datatype = DataTypeHelper.getRDFDataType(sqlType).getURI();
			}
			texprs.addAll(getBaseExpressions(rdfType,
					COL_VAL_RES_LENGTH_LITERAL, sqlType, dth, datatype, lang,
					lanColumn,graph));

			// now create a big concat statement.
			List<Expression> toConcat = new ArrayList<Expression>();
			for (int i = 0; i < altSeq.size(); i++) {
				if (i % 2 == 1) {
					String colName = altSeq.get(i);
					//validate and register the colname first
					dbaccess.getDataType(fi,colName);
					toConcat.add(dth.cast(ColumnHelper.createCol(fi.getAlias(),colName ),dth.getStringCastType()));
				} else {
					toConcat.add(dth.cast(new StringValue("\"" +altSeq.get(i) +  "\""), dth.getStringCastType()));
				}
			}
			
			Expression concat = dth.cast(FilterUtil.concat(toConcat.toArray(new Expression[0])),dth.getStringCastType());
			texprs.add(concat);	

			

		} else if (rdfType.equals(COL_VAL_TYPE_RESOURCE)|| rdfType.equals( COL_VAL_TYPE_BLANK)) {
			if(altSeq.get(0).isEmpty()){
				//we set the base uri 
				altSeq.set(0, baseUri);
			}
			
			
			texprs.addAll(getBaseExpressions(rdfType, altSeq.size(),
					COL_VAL_SQL_TYPE_RESOURCE, dth, null, null, null,graph));
			for (int i = 0; i < altSeq.size(); i++) {
				if (i % 2 == 1) {
					String colName = R2RMLModel.unescape(altSeq.get(i));
					//validate and register the colname first
					dbaccess.getDataType(fi,colName);
					texprs.add(dth.cast(ColumnHelper.createCol(fi.getAlias(), colName),dth.getStringCastType()));
				} else {
					texprs.add(dth.cast(new StringValue("\"" +altSeq.get(i) +  "\""), dth.getStringCastType()));
				}
			}
			

		
		}
		
		// we go now for all unescapeded "{"

		return texprs;

	}

	/*
	 * Here be helper methods.
	 */

	public static Integer getRDFType(RDFNode node) {
		Integer rdfType;
		if (node.isURIResource()) {
			rdfType = COL_VAL_TYPE_RESOURCE;
		} else if (node.isLiteral()) {
			rdfType = COL_VAL_TYPE_LITERAL;
		} else if (node.isAnon()) {
			rdfType = COL_VAL_TYPE_BLANK;
		} else {
			throw new ImplementationException("Encountered unknown Node type");
		}

		return rdfType;
	}

	public Expression asExpression(String string, DataTypeHelper dth) {
		return dth.cast(new StringValue("\"" + string + "\""),
				dth.getStringCastType());
	}
	
	
	public static Column createCol(String tablename, String colname) {
		Column col = new Column();
		col.setColumnName(colname);
		Table tab = new Table();
		tab.setName(tablename);
		tab.setAlias(tablename);
		col.setTable(tab);

		return col;

	}
	
	
	public Expression getGraphExpression(RDFNode graph,DataTypeHelper dth){
		if(graph.isURIResource()){
			if(graph.asResource().getURI().equals(R2RML.defaultGraph)){
				return null;
			}
			
			return dth.cast(new StringValue("\"" + graph.asResource().getURI() + "\""), dth.getStringCastType());
		}else{
			throw new R2RMLValidationException("only URI-Resources allowed for graphs");
		}
		
		
		
	}
	public Expression getGraphExpression(String[] template, FromItem fi, DataTypeHelper dth){
		
		
	
		
		List<String>  altSeq = Arrays.asList(template);
		
		
			// now create a big concat statement.
			List<Expression> toConcat = new ArrayList<Expression>();
			for (int i = 0; i < altSeq.size(); i++) {
				if (i % 2 == 1) {
					String colName = altSeq.get(i);
					
					toConcat.add(dth.cast(ColumnHelper.createCol(fi.getAlias(),colName ),dth.getStringCastType()));
				} else {
					toConcat.add(dth.cast(new StringValue("\"" +altSeq.get(i) +  "\""), dth.getStringCastType()));
				}
			}
			
			return FilterUtil.concat(toConcat.toArray(new Expression[0]));
				
			
		}
	
	public static Column createColumn(String table, String column) {
		return createColumn(null, table, column);
	}

	public static Column createColumn(String schema, String table, String column) {
		Column col = new Column();
		col.setColumnName(column);
		Table tab = new Table();
		tab.setName(table);
		tab.setAlias(table);
		if (schema != null) {
			tab.setSchemaName(schema);

		}
		col.setTable(tab);

		return col;

	}


}
