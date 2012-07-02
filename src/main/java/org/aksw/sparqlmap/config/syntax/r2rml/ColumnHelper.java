package org.aksw.sparqlmap.config.syntax.r2rml;

import static org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil.cast;

import java.util.ArrayList;
import java.util.List;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

import com.hp.hpl.jena.rdf.model.RDFNode;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.FromItem;

public class ColumnHelper {
	public static String R2R_COL_SUFFIX = "_R2R";
	public static String COL_NAME_RDFTYPE = R2R_COL_SUFFIX + "_1_TYPE";
	public static String COL_NAME_RES_LENGTH = R2R_COL_SUFFIX + "_2_RES_LENGTH";
	public static String COL_NAME_SQL_TYPE = R2R_COL_SUFFIX + "_3_SQL_TYPE";
	public static String COL_NAME_LITERAL_TYPE = R2R_COL_SUFFIX + "_4_LIT_TYPE";
	public static String COL_NAME_LITERAL_LANG = R2R_COL_SUFFIX + "_5_LIT_LAN";
	public static String COL_NAME_RESOURCE_COL_SEGMENT = R2R_COL_SUFFIX
			+ "_6_SEG";
	public static String COL_NAME_LITERAL_STRING = R2R_COL_SUFFIX
			+ "_7_LIT_STRING";
	public static String COL_NAME_LITERAL_NUMERIC = R2R_COL_SUFFIX
			+ "_8_LIT_NUM";
	public static String COL_NAME_LITERAL_DATE = R2R_COL_SUFFIX + "_9_LIT_DATE";
	public static String COL_NAME_LITERAL_BOOL = R2R_COL_SUFFIX
			+ "_10_LIT_BOOL";
	public static String COL_NAME_ORDER_BY = R2R_COL_SUFFIX + "_11_OBY";
	public static String COL_NAME_INTERNAL = "BTF";

	public static Integer COL_VAL_TYPE_RESOURCE = 1;
	public static Integer COL_VAL_TYPE_LITERAL = 2;
	public static Integer COL_VAL_TYPE_BLANK = 3;

	public static Integer COL_VAL_SQL_TYPE_RESOURCE = -9999;
	public static Integer COL_VAL_RES_LENGTH_LITERAL = 0;

	public static String colnameBelongsToVar(String colalias) {
		return colalias.substring(0, colalias.indexOf(R2R_COL_SUFFIX));
	}

	public static List<Expression> getBaseExpressions(Integer type,
			Integer resLength, Integer sqlType, DataTypeHelper dth,
			String datatype, String lang, Column langColumn) {
		List<Expression> baseExpressions = new ArrayList<Expression>();
		LongValue typeVal = new LongValue(Integer.toString(type));
		baseExpressions.add(FilterUtil.cast(typeVal, dth.getNumericCastType()));
		LongValue lengthVal = new LongValue(Integer.toString(resLength));
		baseExpressions
				.add(FilterUtil.cast(lengthVal, dth.getNumericCastType()));
		LongValue sqlTypeVal = new LongValue(Integer.toString(sqlType));
		baseExpressions.add(FilterUtil.cast(sqlTypeVal,
				dth.getNumericCastType()));
		if (datatype == null) {
			baseExpressions.add(cast(
					new net.sf.jsqlparser.expression.NullValue(),
					dth.getStringCastType()));
		} else {
			baseExpressions.add(cast(
					new net.sf.jsqlparser.expression.StringValue("\""
							+ datatype + "\""), dth.getStringCastType()));
		}

		if (lang != null) {
			baseExpressions.add(cast(
					new net.sf.jsqlparser.expression.StringValue("\"" + lang
							+ "\""), dth.getStringCastType()));
		} else if (langColumn != null) {
			baseExpressions.add(cast(langColumn, dth.getStringCastType()));
		} else {
			baseExpressions.add(cast(new NullValue(), dth.getStringCastType()));
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
	public static List<Expression> getExpression(RDFNode node,
			DataTypeHelper dth) {
		List<Expression> texprs = new ArrayList<Expression>();

		if (node.isURIResource()) {
			texprs.addAll(getBaseExpressions(COL_VAL_TYPE_RESOURCE, 1,
					COL_VAL_SQL_TYPE_RESOURCE, dth, null, null, null));
			texprs.add(asExpression(node.asResource().getURI(), dth));
		} else if (node.isLiteral()) {
			texprs.addAll(getBaseExpressions(COL_VAL_TYPE_LITERAL, 0,
					COL_VAL_SQL_TYPE_RESOURCE, dth, node.asLiteral()
							.getDatatypeURI(), node.asLiteral().getLanguage(),
					null));
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
	public static List<Expression> getExpression(Column col, Integer rdfType,
			Integer sqlType, String datatype, String lang, Column lanColumn,
			DataTypeHelper dth) {
		List<Expression> texprs = new ArrayList<Expression>();

		if (rdfType == COL_VAL_TYPE_LITERAL) {

			// if datatype not declared, we check, if we got a default
			// conversion
			if (datatype == null
					&& DataTypeHelper.getRDFDataType(sqlType) != null) {
				datatype = DataTypeHelper.getRDFDataType(sqlType).getURI();
			}

			texprs.addAll(getBaseExpressions(rdfType,
					COL_VAL_RES_LENGTH_LITERAL, sqlType, dth, datatype, lang,
					lanColumn));
			dth.getCastTypeString(sqlType);
			texprs.add(FilterUtil.cast(col, dth.getCastTypeString(sqlType)));

		} else if (rdfType == COL_VAL_TYPE_RESOURCE) {

		} else if (rdfType == COL_VAL_TYPE_BLANK) {

		}
		return texprs;

	}

	public static List<Expression> getExpression(String template,
			Integer rdfType, Integer sqlType, String datatype, String lang,
			DataTypeHelper dth, FromItem fi) {
		List<Expression> texprs = new ArrayList<Expression>();

		if (rdfType == COL_VAL_TYPE_LITERAL) {
			// if datatype not declared, we check, if we got a default
			// conversion
			if (datatype == null
					&& DataTypeHelper.getRDFDataType(sqlType) != null) {
				datatype = DataTypeHelper.getRDFDataType(sqlType).getURI();
			}

			throw new ImplementationException(
					"no Literal templates now, but should not be much of a problem");

		} else if (rdfType == COL_VAL_TYPE_RESOURCE) {

		} else if (rdfType == COL_VAL_TYPE_BLANK) {

		}

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

	public static Expression asExpression(String string, DataTypeHelper dth) {
		return FilterUtil.cast(new StringValue("\"" + string + "\""),
				dth.getStringCastType());
	}

}
