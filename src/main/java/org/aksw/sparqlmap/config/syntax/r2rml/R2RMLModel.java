package org.aksw.sparqlmap.config.syntax.r2rml;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.JSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.config.syntax.DBConnectionConfiguration;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;

import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.reasoner.Reasoner;
import com.hp.hpl.jena.reasoner.ReasonerRegistry;
import com.hp.hpl.jena.sparql.modify.GraphStoreUtils;
import com.hp.hpl.jena.update.GraphStoreFactory;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.util.FileManager;

public class R2RMLModel {
	String r2rmlSchemaLocation = "mapping/r2rml.rdf";
	
	
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(R2RMLModel.class);
	Model mapping = null;
	Model r2rmlSchema = null;
	Model reasoningModel = null;
	
	DBConnectionConfiguration dbconf;
	private DataTypeHelper dth;
	
	public R2RMLModel(Model mapping, Model schema,DBConnectionConfiguration dbconf){
		this.mapping = mapping;
		this.r2rmlSchema = schema;
		this.dbconf = dbconf;
		this.dth = dbconf.getDataTypeHelper();

		reasoningModel = ModelFactory.createRDFSModel(r2rmlSchema,mapping);	
		resolveR2RMLShortcuts();

	}
	
	
	private void resolveR2RMLShortcuts() {
		String query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:subjectMap [ rr:constant ?y ]. } WHERE {?x rr:subject ?y.}";
		UpdateExecutionFactory.create(UpdateFactory.create(query),GraphStoreFactory.create(reasoningModel)).execute();
		query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:predicateMap [ rr:constant ?y ]. } WHERE {?x rr:predicate ?y.}";
		UpdateExecutionFactory.create(UpdateFactory.create(query),GraphStoreFactory.create(reasoningModel)).execute();
		query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:objectMap [ rr:constant ?y ]. } WHERE {?x rr:object ?y.}";
		UpdateExecutionFactory.create(UpdateFactory.create(query),GraphStoreFactory.create(reasoningModel)).execute();
		query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:graphMap [ rr:constant ?y ]. } WHERE {?x rr:graph ?y.}";
		UpdateExecutionFactory.create(UpdateFactory.create(query),GraphStoreFactory.create(reasoningModel)).execute();
		reasoningModel.size();

	}





	
	public void getVirtualTables(){
		
	}
	

	public String toTurtle(){
		return null;
	}
	
	
	private int queryCount = 1;
	
	

	
	
	public Set<TripleMap> getTripleMaps() throws R2RMLValidationException, JSQLParserException{
		Set<TripleMap> tripleMaps = new HashSet<TripleMap>();
		
	
		String tmquery = "PREFIX rr: <http://www.w3.org/ns/r2rml#> SELECT ?tm ?tableName ?query ?version {?tm a rr:TriplesMap. ?tm rr:logicalTable ?tab . {?tab rr:tableName ?tableName} UNION {?tab rr:sqlQuery ?query. OPTIONAL{?tab rr:sqlVersion ?version}}}" ;
		ResultSet tmrs = QueryExecutionFactory.create(QueryFactory.create(tmquery), reasoningModel).execSelect();
		
		while(tmrs.hasNext()){
			FromItem fromItem;
			Table fromTable;
			SubSelect subsel;
			
			QuerySolution solution = tmrs.next();
			Resource tmUri= solution.get("tm").asResource();
			String tablename = solution.get("?tableName")!=null?solution.get("?tableName").asLiteral().getString():null;
			String query = solution.get("?query")!=null?solution.get("?query").asLiteral().getString():null;
			String version  = solution.get("?version")!=null?solution.get("?version").asLiteral().getString():null;
			
			
			
			
			
			
			if(tablename!=null&&query==null&&version==null){
				fromTable = new Table(null,tablename);
				fromItem = fromTable;
			}else if(tablename ==null && query!=null){
				subsel = new SubSelect();
				subsel.setAlias("query_" + queryCount++);
				subsel.setSelectBody(((Select)new CCJSqlParserManager().parse(new StringReader(query))).getSelectBody());				
				fromTable = new Table(null, subsel.getAlias());
				fromItem = subsel;
				
			}else{
				throw new R2RMLValidationException("Odd virtual table declaration in term map: " + tmUri.toString());
			}
			
			//validate fromItem
			
			dbconf.validateFromItem(fromItem);
			
			
			
			TripleMap triplemap = new TripleMap(fromItem);
			
			
			
			//get the s-term
			String squery  = "PREFIX rr: <http://www.w3.org/ns/r2rml#> SELECT" +
					" * {" +
					" <"+tmUri.getURI()+"> rr:subjectMap ?sm  " +
					getTermMapQuery("s")+ 
					"}";
			
					//"{?sm rr:column ?column} UNION {?sm rr:constant ?constant} UNION {?sm rr:template ?template} OPTIONAL {?sm rr:class ?class} OPTIONAL {?sm rr:termType ?termtype}}" ;
			ResultSet srs = QueryExecutionFactory.create(QueryFactory.create(squery), this.mapping).execSelect();
			// there should only be one 
			if(srs.hasNext()==false){
				throw new R2RMLValidationException("Triple map " +tmUri+ " has no subject term map, fix this");
			}
			QuerySolution sSoltution = srs.next();
			TermMapQueryResult sres=  new TermMapQueryResult(sSoltution, "s");
			
			if(srs.hasNext() == true){
				throw new R2RMLValidationException("Triple map " +tmUri+ " has more than ob subject term map, fix this");
			}
			
			
//			String stemplate = 
//			String scolumnName = sSoltution.get("?column")!=null?sSoltution.get("?column").asLiteral().getString():null;
//			RDFNode snode = sSoltution.get("?constant");
//			Resource tmClass = sSoltution.getResource("?class");
//			Resource termType = sSoltution.getResource("?termtype");
			int nodeType;
			if(sres.termType==null||sres.termType.hasURI(R2RML.IRI)){
				sres.termTypeInt = ColumnHelper.COL_VAL_TYPE_RESOURCE;
			}else{
				sres.termTypeInt = ColumnHelper.COL_VAL_TYPE_BLANK;
			}
			
			
			TermMap stm = null;
			if(sres.template!=null){
				List<Expression> stmExpressions = ColumnHelper.getExpression(sres.template, ColumnHelper.COL_VAL_TYPE_RESOURCE, null,null, null, dth,fromItem);
				stm = new TermMap(dth,stmExpressions,Arrays.asList(fromItem), null,triplemap);
			}else if(sres.column!=null){
				Column col = new Column();
				col.setTable(fromTable);
				col.setColumnName(sres.column);
				List<Expression> stmExpressions = ColumnHelper.getExpression(col, ColumnHelper.COL_VAL_TYPE_RESOURCE, null, null, null,null, dth);			
				stm = new TermMap(dth,stmExpressions,Arrays.asList(fromItem),null,triplemap);
			}
			
			triplemap.subject = stm;
			
			
			//get the POs
			
			String poQuery = "PREFIX rr: <http://www.w3.org/ns/r2rml#> " +
					"SELECT * {" + //?ptemplate ?pcolumn ?pconstant ?ptermtype ?otemplate ?ocolumn ?oconstant ?otermtype ?olang ?odatatype {" +
					"<"+tmUri.getURI()+"> <"+R2RML.predicateObjectMap+"> ?pom. ?pom <"+R2RML.predicateMap+"> ?pm . ?pom <"+R2RML.objectMap+"> ?om." +
					getTermMapQuery("p") + 
					getTermMapQuery("o") + 
					"}";
			
			
			ResultSet pors = QueryExecutionFactory.create(QueryFactory.create(poQuery), reasoningModel).execSelect();
			while(pors.hasNext()){
				QuerySolution posol = pors.next();
				TermMapQueryResult p = new TermMapQueryResult(posol,"p");
				TermMap ptm;
				
				
				//some general validation
				if(p.termType!=null&&!p.termType.getURI().equals(R2RML.IRI)){
					throw new R2RMLValidationException("Only use iris in predicate position");
				}
				
				
				if(p.column!=null){
					// generate from colum
					Column col = new Column();
					col.setTable(fromTable);
					col.setColumnName(p.column);
					List<Expression> pexprs = ColumnHelper.getExpression(col, ColumnHelper.COL_VAL_TYPE_RESOURCE, ColumnHelper.COL_VAL_SQL_TYPE_RESOURCE, null, null, null, dth);
					ptm = new TermMap(dth, pexprs, Arrays.asList(fromItem), null, triplemap);
				}else if(p.constant != null){
					//use constant term
					if(!p.constant.isURIResource()){
						throw new R2RMLValidationException("Must IRI in predicate position");
					}
					List<Expression> pexprs = ColumnHelper.getExpression(p.constant, dth);
					ptm = new TermMap(dth, pexprs, Arrays.asList(fromItem), null, triplemap);
				}else if(p.template!=null){
					//from template
					List<Expression> ptmExpressions = ColumnHelper.getExpression(sres.template, ColumnHelper.COL_VAL_TYPE_RESOURCE, null,null, null, dth,fromItem);
					stm = new TermMap(dth,ptmExpressions,Arrays.asList(fromItem), null,triplemap);
				}else{
					throw new R2RMLValidationException("Invalid predicate declaration encountered");
				}
					
				
				
				
				TermMapQueryResult o = new TermMapQueryResult(posol,"o");
				Integer otermtype = ColumnHelper.COL_VAL_TYPE_RESOURCE;
				
				
				if(o.column!=null||o.lang!=null||o.datatypeuri !=null){
					otermtype = ColumnHelper.COL_VAL_TYPE_LITERAL;
				}else if(o.termType.getURI().equals(R2RML.BlankNode)){
					otermtype = ColumnHelper.COL_VAL_TYPE_BLANK;
				}
				
				
				
				
				if(o.column!=null){
					// generate from colum
					Column col = new Column();
					col.setTable(fromTable);
					col.setColumnName(o.column);
					List<Expression> pexprs = ColumnHelper.getExpression(col,otermtype, ColumnHelper.COL_VAL_SQL_TYPE_RESOURCE, null, null, null, dth);
					ptm = new TermMap(dth, pexprs, Arrays.asList(fromItem), null, triplemap);
				}else if(o.constant != null){
					//use constant term
			
					List<Expression> pexprs = ColumnHelper.getExpression(o.constant, dth);
					ptm = new TermMap(dth, pexprs, Arrays.asList(fromItem), null, triplemap);
				}else if(o.template!=null){
					//from template
					List<Expression> ptmExpressions = ColumnHelper.getExpression(sres.template, otermtype, null,null, null, dth,fromItem);
					stm = new TermMap(dth,ptmExpressions,Arrays.asList(fromItem), null,triplemap);
				}
				
			}
			
			
			
			
		}
		
	
		
		return tripleMaps;
	}
	
	
	/**
	 * creates a part of a query that creates 
	 * @param prefix
	 * @return
	 */
	private String getTermMapQuery(String prefix){
		String p = prefix;
		String query = "{?"+p+"m rr:column ?"+p+"column} " +
				"UNION {?"+p+"m rr:constant ?"+p+"constant} " +
						"UNION {?"+p+"m rr:template ?"+p+"template} " +
								"OPTIONAL {?"+p+"m rr:class ?"+p+"tmclass} " +
										"OPTIONAL {?"+p+"m rr:termType ?"+p+"termtype} " +
										"OPTIONAL {?"+p+"m rr:datatype ?"+p+"datatype} " +
												"OPTIONAL {?"+p+"m <"+R2RML.language+"> ?"+p+"lang} " +
														"OPTIONAL {?"+p+"m <"+R2RML.inverseExpression+"> ?"+p+"inverseexpression}";
		
		return query;
	}
	
	
	
	private class TermMapQueryResult{
		
		public TermMapQueryResult(QuerySolution sol, String prefix) {
			template = sol.get("?"+prefix+"template")!=null?sol.get("?"+prefix+"template").asLiteral().getString():null;
			column = sol.get("?"+prefix+"column")!=null?sol.get("?"+prefix+"column").asLiteral().getString():null;
			lang = sol.get("?"+prefix+"lang")!=null?sol.get("?"+prefix+"lang").asLiteral().getString():null;
			inverseExpression = sol.get("?"+prefix+"inverseexpression")!=null?sol.get("?"+prefix+"inverseexpression").asLiteral().getString():null;
			constant = sol.get("?"+prefix+"constant");
			datatypeuri = sol.get("?"+prefix+"datatypeuri")!=null?sol.get("?"+prefix+"datatypeuri").asResource():null;
			tmclass = sol.get("?"+prefix+"tmclass")!=null?sol.get("?"+prefix+"tmclass").asResource():null;
			termType = sol.getResource("?termtype");
		}
		
		String template;
		String column;
		RDFNode constant;
		String lang;
		Resource datatypeuri;
		String inverseExpression;
		Resource tmclass;
		Resource termType;
		int termTypeInt;
	}
	
	
	
private List<String> splitTemplate(String template) {
	
		
		String[] ldsplits = template.split("\\{|\\}");
		return Arrays.asList(ldsplits);
	

	}


public Integer getSqlDataType(String tablename, String colname) {
	// TODO Auto-generated method stub
	return null;
}
	
//	public List<Expression> getResourceExpressions(ColumDefinition coldef) {
//			
//			DataTypeHelper dth = config.getDbConn().getDataTypeHelper();
//			List<String> resourceSegments = getResourceSegements(coldef);
//			List<Expression> expressions = ColumnHelper.getBaseExpressions(1, resourceSegments.size(), ColumnHelper.COL_SQL_TYPE_RESOURCE,config.getDbConn().getDataTypeHelper());
//			for (int i = 0; i < resourceSegments.size(); i++) {
//				if (i % 2 == 0) {
//					// we have a string
//					expressions.add(FilterUtil.cast(new net.sf.jsqlparser.expression.StringValue(
//							"'" + resourceSegments.get(i) + "'"),dth.getStringCastType()));
//				} else {
//					// string denominates a column
//					String[] colname = resourceSegments.get(i).split("\\.");
//					if (colname.length != 2) {
//						throw new ImplementationException(
//								resourceSegments.get(i)
//										+ " was split in more than 2 segments. Either schema is present (not supported atm ) or sth else");
//					}
//	
//					if (colname[0].equals(coldef.getMapp().getName())) {
//						expressions.add(FilterUtil.cast(coldef.getMapp()
//								.getColumnDefinition(colname[1]).getColumn(),dth.getStringCastType()));
//					} else {
//						// need to get resource creation column from
//						ColumDefinition rCol = config.getMappingConfiguration().getColumnForName(colname[0],
//								colname[1]);
//						expressions.add(FilterUtil.cast(rCol.colum,dth.getStringCastType()));
//					}
//				}
//			}
//			return expressions;
//		}
//	
//	public List<Expression> getLiteralExpressions(ColumDefinition coldef){
//		List<Expression> literalExpressions = ColumnHelper.getBaseExpressions(2, 0, coldef.getSqldataType(),config.getDbConn().getDataTypeHelper());
//		
//		
//		Expression castedcol = FilterUtil.cast(coldef.getColumn(), config.getDbConn().getDataTypeHelper().getCastTypeString(coldef.getSqldataType()));
//		literalExpressions.add(castedcol);
//
//		
//		return literalExpressions;
//	}
//	
//	private List<String> getResourceSegements(ColumDefinition coldef) {
//		List<String> segs = new ArrayList<String>();
//		Mapping mapOfCol = coldef.getMapp();
//		if (coldef instanceof ConstantValueColumn) {
//			segs.add(((ConstantValueColumn) coldef).getValue());
//		} else if (mapOfCol.getIdColumn().equals(coldef)) {
//			// use the template of the mapping
//			segs.addAll(splitTemplate(mapOfCol.getIdColumn().getUriTemplate()));
//
//		} else if (coldef.getJoinsAlias() != null) {
//			// use segments of the referenced table
//			Mapping joinWithMapp = config.getMappingConfiguration().getMappingForAlias(
//					coldef.getJoinsAlias());
//			ColumDefinition joinWithIdColumDefinition = joinWithMapp.getIdColumn();
//			//we check if the colums  are constructable from the foreign key we got here.
//			//this is the case, if the template of the referenced table is contains only its id cols 
//						
//			List<String> joinWithSegs = splitTemplate(joinWithIdColumDefinition.getUriTemplate());
//			
//			
//			if(joinWithSegs.get(1).toString().equals(joinWithIdColumDefinition.getColumn().getTable().getAlias() +"." + joinWithIdColumDefinition.getColumn().getColumnName())){
//				//they are, we can replace this segment with this column
//				joinWithSegs.set(1, coldef.getColumn().getTable().getAlias() +"." + coldef.getColumn().getColumnName());
//			}
//			
//			segs.addAll(joinWithSegs);
//
//		} else if (coldef.getUriTemplate() != null) {
//			if (!coldef.getUriTemplate().contains("{")) {
//				coldef.setUriTemplate( coldef.getUriTemplate()+"{" + coldef.getMapp().getFromPart().getAlias() + "."
//						+ coldef.getColname() + "}");
//			}
//			// this column itself produces an iri
//			segs.addAll(splitTemplate(coldef.getUriTemplate()));
//		}
//		return segs;
//	}
//	
//	
//	
	
	
	


}
