package org.aksw.sparqlmap.core.config.syntax.r2rml;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.FilterUtil;
import org.aksw.sparqlmap.core.mapper.translate.ImplementationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.vocabulary.RDFS;


@Component
public class ColumnHelper {
	
	public static String R2R_COL_SUFFIX = "_R2R";
	public static String COL_NAME_RDFTYPE = R2R_COL_SUFFIX + "_01TP";
	public static String COL_NAME_LITERAL_TYPE = R2R_COL_SUFFIX + "_02LITT";
	public static String COL_NAME_LITERAL_LANG = R2R_COL_SUFFIX + "_03LITL";
	public static String COL_NAME_LITERAL_STRING = R2R_COL_SUFFIX
			+ "_04LS";
	public static String COL_NAME_LITERAL_NUMERIC = R2R_COL_SUFFIX
			+ "_05LN";
	public static String COL_NAME_LITERAL_DATE = R2R_COL_SUFFIX + "_6LD";
	public static String COL_NAME_LITERAL_BOOL = R2R_COL_SUFFIX
			+ "_07LB";
	public static String COL_NAME_LITERAL_BINARY = R2R_COL_SUFFIX + "08_L0";
	public static String COL_NAME_RESOURCE_COL_SEGMENT = R2R_COL_SUFFIX
			+ "_09R";
	public static String COL_NAME_ORDER_BY = R2R_COL_SUFFIX + "_XXOB";
	
	public static String COL_NAME_INTERNAL = "B";

	
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
	DBAccess dbaccess;
	
	@Autowired
	DataTypeHelper dth;

//	public List<Expression> getBaseExpressions(Integer type,
//			Integer resLength, 
//			String datatype, String lang, Column langColumn) {
//		List<Expression> baseExpressions = new ArrayList<Expression>();
//		LongValue typeVal = new LongValue(Integer.toString(type));
//		baseExpressions.add(dth.cast(typeVal, dth.getNumericCastType()));
//		LongValue lengthVal = new LongValue(Integer.toString(resLength));
//		baseExpressions
//				.add(dth.cast(lengthVal, dth.getNumericCastType()));
//
//		if (datatype == null) {
//			if(type==COL_VAL_TYPE_LITERAL){
//				baseExpressions.add(dth.cast(
//						new StringValue("\""+RDFS.Literal.getURI()+"\""),
//						dth.getStringCastType()));
//			}else{
//			baseExpressions.add(dth.cast(
//					new StringValue("\"" +RDFS.Resource.getURI() + "\""),
//					dth.getStringCastType()));
//			}
//		} else {
//			baseExpressions.add(dth.cast(
//					new StringValue("\""
//							+ datatype + "\""), dth.getStringCastType()));
//		}
//
//		if (lang != null) {
//			if(lang.length()!=2&&!lang.toLowerCase().equals(lang)){
//				throw new R2RMLValidationException("language string must be a two letter code in lower cases");
//			}
//			baseExpressions.add(dth.cast(
//					new StringValue("\"" + lang
//							+ "\""), dth.getStringCastType()));
//		} else if (langColumn != null) {
//			baseExpressions.add(dth.cast(langColumn, dth.getStringCastType()));
//		} else {
//			if(type==COL_VAL_TYPE_LITERAL){
//				baseExpressions.add(dth.cast(
//						new StringValue("\"" +RDFS.Literal.getURI() + "\""),
//						dth.getStringCastType()));
//			}else{
//			baseExpressions.add(dth.cast(
//					new StringValue("\"" +RDFS.Resource.getURI()+"\""),
//					dth.getStringCastType()));
//			}
//		}
//	
//		return baseExpressions;
//	}
//	
	
	
//	
//
//	/**
//	 * create the Expressions for a constant values term map
//	 * 
//	 * @param node
//	 * @param dth
//	 * @return
//	 */
//	public List<Expression> getExpression(Node node,
//			DataTypeHelper dth) {
//		List<Expression> texprs = new ArrayList<Expression>();
//
//		if (node.isURI()) {
//			texprs.addAll(getBaseExpressions(COL_VAL_TYPE_RESOURCE, 1,
//					 null, null, null));
//			//texprs.add(dth.cast(new StringValue("\"\""), dth.getStringCastType()));
//			texprs.add(asExpression(node.getURI(), dth));
//		} else if (node.isLiteral()) {
//			texprs.addAll(getBaseExpressions(COL_VAL_TYPE_LITERAL, 0,
//					  node.getLiteralDatatype().toString(), node.getLiteralLanguage(),
//					null));
//			
//			//get the cast type
//			
//			Literal nodeL = (Literal) node;
//
//			texprs.add(dth.cast(new StringValue("\"" + nodeL.getLexicalForm() + "\""), dth.getCastTypeString(nodeL.getDatatype())));
//		} else {
//			throw new ImplementationException(
//					"No support for constant blank nodes in SparqlMap");
//		}
//
//		return texprs;
//	}

//	
//
//	/**
//	 * Create the expressions for a term map, that is producing a literal, based
//	 * on columns.
//	 * 
//	 * @param col
//	 * @param sqlType
//	 * @param datatype
//	 * @param lang
//	 * @param lanColumn
//	 * @param dth
//	 * @return
//	 */
//	public List<Expression> getExpression(Column col, Integer rdfType,
//			Integer sqlType, String datatype, String lang, Column lanColumn,
//			DataTypeHelper dth) {
//		List<Expression> texprs = new ArrayList<Expression>();
//
//		if (rdfType.equals(COL_VAL_TYPE_LITERAL)) {
//
//			// if datatype not declared, we check, if we got a default
//			// conversion
//			if (datatype == null
//					&& DataTypeHelper.getRDFDataType(sqlType) != null) {
//				datatype = DataTypeHelper.getRDFDataType(sqlType).getURI();
//			}
//
//			texprs.addAll(getBaseExpressions(rdfType,
//					COL_VAL_RES_LENGTH_LITERAL, datatype, lang,
//					null));
//			RDFDatatype rdfdt = tm.getTypeByName(datatype); 
//			if(datatype==null||rdfdt==null){
//				texprs.add(dth.cast(col, dth.getCastTypeString(sqlType)));
//			}else{
//				texprs.add(dth.cast(col, dth.getCastTypeString(rdfdt)));
//			}
//
//		} else if (rdfType.equals(COL_VAL_TYPE_RESOURCE)||rdfType.equals(COL_VAL_TYPE_BLANK)) {
//			texprs.addAll(getBaseExpressions(rdfType,
//					1,  datatype, lang,
//					null));
////			texprs.add(dth.cast(new StringValue("\"\""), dth.getStringCastType()));
//			texprs.add(dth.cast(col, dth.getStringCastType()));
//
//		} 
//		return texprs;
//
//	}
//	
//	TypeMapper tm = TypeMapper.getInstance();
//	
	

//
//	public  List<Expression> getExpression(String[] template,
//			Integer rdfType, int sqlType, String datatype, String lang, Column lanColumn, 
//			DataTypeHelper dth, FromItem fi,Expression graph,String baseUri) {
//		List<Expression> texprs = null;
//		List<String>  altSeq = Arrays.asList(template);
//		
//
//		
//		if (rdfType.equals(COL_VAL_TYPE_LITERAL)) {
//			texprs = getExpressionsForTemplateLiteral(rdfType, sqlType, datatype, lang,
//					lanColumn,  fi, graph, altSeq);
//		}
//		if (rdfType.equals(COL_VAL_TYPE_RESOURCE)) {
//			texprs = getExpressionsForTemplateResource(
//					rdfType,  fi, graph, baseUri, altSeq);
//			
//		} 
//		if ( rdfType.equals( COL_VAL_TYPE_BLANK)){
//			texprs = getExpressionsForTemplateBlankNode(
//					rdfType,  fi, graph, baseUri, altSeq);
//		}
//	
//		return texprs;
//
//	}
//	private List<Expression> getExpressionsForTemplateBlankNode(
//			Integer rdfType, FromItem fi,
//			Expression graph, String baseUri, List<String> altSeq) {
//		
//		
//		if(dth.hasRowIdFunction()){
//			List<Expression> texprs =  new ArrayList<Expression>();
//			texprs.addAll(getBaseExpressions(rdfType, 2,
//					null, null, null));
//			//first an empty string to maintain the string-column scheme
//			texprs.addAll(dth.getRowIdFunction(fi.getAlias()));
//			return texprs;
//			
//		}else{
//			return getExpressionsForTemplateResource(rdfType, fi, graph, baseUri, altSeq);
//		}
//	}
//	private List<Expression> getExpressionsForTemplateLiteral(Integer rdfType, int sqlType,
//			String datatype, String lang, Column lanColumn, 
//			FromItem fi, Expression graph,
//			List<String> altSeq) {
//		List<Expression> texprs =  new ArrayList<Expression>();
//		// if datatype not declared, we check, if we got a default
//		// conversion
//		if (datatype == null
//				&& DataTypeHelper.getRDFDataType(sqlType) != null) {
//			datatype = DataTypeHelper.getRDFDataType(sqlType).getURI();
//		}
//		texprs.addAll(getBaseExpressions(rdfType,
//				COL_VAL_RES_LENGTH_LITERAL, datatype, lang,
//				lanColumn));
//
//		// now create a big concat statement.
//		List<Expression> toConcat = new ArrayList<Expression>();
//		for (int i = 0; i < altSeq.size(); i++) {
//			if (i % 2 == 1) {
//				String colName = altSeq.get(i);
//				//validate and register the colname first
//				dbaccess.getDataType(fi,colName);
//				toConcat.add(dth.cast(ColumnHelper.createCol(fi.getAlias(),colName ),dth.getStringCastType()));
//			} else {
//				toConcat.add(dth.cast(new StringValue("\"" +altSeq.get(i) +  "\""), dth.getStringCastType()));
//			}
//		}
//		
//		Expression concat = dth.cast(FilterUtil.concat(toConcat.toArray(new Expression[0])),dth.getStringCastType());
//		texprs.add(concat);
//		
//		
//		return texprs;
//	}
	
	
//	
//	private List<Expression> getExpressionsForTemplateResource(Integer rdfType,
//			 FromItem fi, Expression graph, String baseUri,
//			List<String> altSeq) {
//		List<Expression> newExprs = new ArrayList<Expression>();
//		
//		if(altSeq.get(0).isEmpty()){
//			//we set the base uri 
//			altSeq.set(0, baseUri);
//		}
//		newExprs.addAll(getBaseExpressions(rdfType, altSeq.size(),
//				null, null, null));
//		for (int i = 0; i < altSeq.size(); i++) {
//			if (i % 2 == 1) {
//				String colName = R2RMLModel.unescape(altSeq.get(i));
//				//validate and register the colname first
//				//dbaccess.getDataType(fi,colName);
//				newExprs.add(dth.cast(ColumnHelper.createCol(fi.getAlias(), colName),dth.getStringCastType()));
//			} else {
//				newExprs.add(dth.cast(new StringValue("\"" +altSeq.get(i) +  "\""), dth.getStringCastType()));
//			}
//		}
//		return newExprs;
//	}

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
//
//	public void createBooleanExpressions(Expression boolExpr, DataTypeHelper dth){
//
//		
//		getBaseExpressions(COL_VAL_TYPE_LITERAL, 0,  XSDDatatype.XSDboolean.toString(), null, null);
//		
//		//return getExpression(boolExpr, dth);
//	}


	
	public static Expression getTermType(List<Expression> expressions){
		return expressions.get(0);
	}
	public static Expression  getDataType(List<Expression> expressions){
		return expressions.get(1);	
	}
	public static Expression getLanguage(List<Expression> expressions){
		return expressions.get(2);
	}
	
	public static Expression getLiteralStringExpression(List<Expression> expressions){
		return expressions.get(3);
	}
	public static Expression getLiteralNumericExpression(List<Expression> expressions){
		return expressions.get(4);
	}
	
	public static Expression getLiteralDateExpression(List<Expression> expressions){
		return expressions.get(5);
	}
	public static Expression getLiteralBoolExpression(List<Expression> expressions) {
		
		return expressions.get(6);
	}
	
	public static Expression getLiteralBinaryExpression(List<Expression> expressions){
		return expressions.get(7);
	}
	

	public static List<Expression> getResourceExpressions(List<Expression> expressions){
		
		return expressions.subList(8, expressions.size());
	}
	public static List<Expression> getLiteralExpression(List<Expression> expressions){
		List<Expression> exps =  new ArrayList<Expression>();
		exps.add(getLiteralStringExpression(expressions));
		exps.add(getLiteralNumericExpression(expressions));
		exps.add(getLiteralDateExpression(expressions));
		exps.add(getLiteralBoolExpression(expressions));
		exps.add(getLiteralBinaryExpression(expressions));
			
		return exps;
	}

}
