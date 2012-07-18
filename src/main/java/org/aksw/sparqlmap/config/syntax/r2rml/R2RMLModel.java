package org.aksw.sparqlmap.config.syntax.r2rml;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.ExplicitSelectBody;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.columnanalyze.CompatibilityChecker;
import org.aksw.sparqlmap.columnanalyze.CompatibilityCheckerFactory;
import org.aksw.sparqlmap.config.syntax.DBConnectionConfiguration;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.query.regexptrees.Alternatives;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.update.GraphStoreFactory;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.vocabulary.RDF;

public class R2RMLModel {
	
	
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(R2RMLModel.class);
	Model mapping = null;
	Model r2rmlSchema = null;
	Model reasoningModel = null;
	DBConnectionConfiguration dbconf;
	
	Set<TripleMap> tripleMaps = null;
	
	
	private DataTypeHelper dth;
	
	public R2RMLModel(Model mapping, Model schema,DBConnectionConfiguration dbconf) throws R2RMLValidationException, JSQLParserException{
		this.mapping = mapping;
		this.r2rmlSchema = schema;
		this.dbconf = dbconf;
		this.dth = dbconf.getDataTypeHelper();

		reasoningModel = ModelFactory.createRDFSModel(r2rmlSchema,mapping);	
		resolveRRClassStatements();
		resolveR2RMLShortcuts();
		loadTripleMaps();
		
	
		
		loadCompatibilityChecker();

	}
	
	
	private void resolveRRClassStatements() {
		String query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> " +
				"INSERT { ?tm rr:predicateObjectMap  _:newpo. " +
				"_:newpo rr:predicate <"+RDF.type.getURI()+">." +
				"_:newpo rr:object ?class } " +
				"WHERE {?tm a rr:TriplesMap." +
				"?tm  rr:subjectMap ?sm." +
				"?sm rr:class ?class }";
		UpdateExecutionFactory.create(UpdateFactory.create(query),GraphStoreFactory.create(reasoningModel)).execute();
		
	}


	private void loadCompatibilityChecker() {
		CompatibilityCheckerFactory ccfac = new CompatibilityCheckerFactory(reasoningModel,dbconf);
		
		for (TripleMap tripleMap : tripleMaps) {
			CompatibilityChecker ccs = ccfac.createCompatibilityChecker(tripleMap.getSubject().getExpressions());
			tripleMap.getSubject().setCompChecker(ccs);
			
			for(PO po :tripleMap.pos){
				CompatibilityChecker ccp = ccfac.createCompatibilityChecker(po.getPredicate().getExpressions());
				po.getPredicate().setCompChecker(ccp);
				CompatibilityChecker cco = ccfac.createCompatibilityChecker(po.getObject().getExpressions());
				po.getObject().setCompChecker(cco);
			}
		}
		
		
		
		
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
	
	
	
	public Set<TripleMap> getTripleMaps(){

		
		return Collections.unmodifiableSet(tripleMaps);
	}
	
	

	
	
	private  void loadTripleMaps() throws R2RMLValidationException, JSQLParserException{
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
			Resource version  = solution.get("?version")!=null?solution.get("?version").asResource():null;
			
			
			
			
			
			
			if(tablename!=null&&query==null&&version==null){
				tablename = unescape(tablename);
				
				
				fromTable = new Table(null,tablename);
				fromItem = fromTable;
				fromTable.setAlias(tablename);
			}else if(tablename ==null && query!=null){
				query= cleanSql(query);
				subsel = new SubSelect();
				subsel.setAlias("query_" + queryCount++);
				//subsel.setSelectBody(((Select)new CCJSqlParserManager().parse(new StringReader(query))).getSelectBody());
				subsel.setSelectBody(new ExplicitSelectBody(query));
				fromTable = new Table(null, subsel.getAlias());
				fromTable.setAlias(subsel.getAlias());
				fromItem = subsel;
				
			}else{
				throw new R2RMLValidationException("Odd virtual table declaration in term map: " + tmUri.toString());
			}
			
			//validate fromItem
			
			try {
				dbconf.validateFromItem(fromItem);
			} catch (SQLException e) {
				throw new R2RMLValidationException("Error validation the virtual table in mapping" + tmUri.getURI(),e);
			}
			
			
			
			TripleMap triplemap = new TripleMap(tmUri.getURI(), fromItem);
			
			
			
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
			TermMapQueryResult sres=  new TermMapQueryResult(sSoltution, "s",fromItem);
			
			if(srs.hasNext() == true){
				throw new R2RMLValidationException("Triple map " +tmUri+ " has more than one subject term map, fix this");
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
				List<Expression> stmExpressions = ColumnHelper.getExpression(sres.template, sres.termTypeInt, null,null, null,null, dth,fromItem);
				stm = new TermMap(dth,stmExpressions,Arrays.asList(fromItem), null,triplemap);
			}else if(sres.column!=null){
				Column col = new Column();
				col.setTable(fromTable);
				col.setColumnName(sres.column);
				List<Expression> stmExpressions = ColumnHelper.getExpression(col, sres.termTypeInt, null, null, null,null, dth);			
				stm = new TermMap(dth,stmExpressions,Arrays.asList(fromItem),null,triplemap);
			}else if(sres.constant!=null){
				if(!sres.constant.isURIResource()){
					throw new R2RMLValidationException("Must IRI in predicate position");
				}
				List<Expression> sexprs = ColumnHelper.getExpression(sres.constant, dth);
				stm = new TermMap(dth, sexprs, Arrays.asList(fromItem), null, triplemap);
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
			
				TermMapQueryResult p = new TermMapQueryResult(posol,"p",fromItem);
				TermMap ptm = null;
				
				
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
					List<Expression> ptmExpressions = ColumnHelper.getExpression(sres.template, ColumnHelper.COL_VAL_TYPE_RESOURCE, null,null, null,null, dth,fromItem);
					stm = new TermMap(dth,ptmExpressions,Arrays.asList(fromItem), null,triplemap);
				}else{
					throw new R2RMLValidationException("Invalid predicate declaration encountered");
				}
					
				
				
				
				TermMapQueryResult o = new TermMapQueryResult(posol,"o",fromItem);
				Integer otermtype = ColumnHelper.COL_VAL_TYPE_RESOURCE;
				TermMap otm=  null;
				
				if(o.column!=null||o.lang!=null||o.datatypeuri !=null){
					otermtype = ColumnHelper.COL_VAL_TYPE_LITERAL;
				}else if(o.termType!=null&&o.termType.getURI().equals(R2RML.BlankNode)){
					otermtype = ColumnHelper.COL_VAL_TYPE_BLANK;
				}
				
				
				
				
				if(o.column!=null){
					// generate from colum
					Column col = new Column();
					col.setTable(fromTable);
					col.setColumnName(o.column);
					List<Expression> oexprs = ColumnHelper.getExpression(col,otermtype,dbconf.getDataType(fromItem, o.column), null, null, null, dth);
					otm = new TermMap(dth, oexprs, Arrays.asList(fromItem), null, triplemap);
				}else if(o.constant != null){
					//use constant term
			
					List<Expression> oexprs = ColumnHelper.getExpression(o.constant, dth);
					otm = new TermMap(dth, oexprs, Arrays.asList(fromItem), null, triplemap);
				}else if(o.template!=null){
					//from template
					List<Expression> otmExpressions = ColumnHelper.getExpression(o.template, otermtype, null,null, null,null, dth,fromItem);
					otm = new TermMap(dth,otmExpressions,Arrays.asList(fromItem), null,triplemap);
				}
				
				
				triplemap.addPO(ptm, otm);
				
				
				
				
				
			
				
			}
			
			tripleMaps.add(triplemap);
			
			
		}
		
	
		
		this.tripleMaps =  tripleMaps;
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
				"OPTIONAL {?"+p+"m rr:termType ?"+p+"termtype} " +
										"OPTIONAL {?"+p+"m rr:datatype ?"+p+"datatype} " +
												"OPTIONAL {?"+p+"m <"+R2RML.language+"> ?"+p+"lang} " +
														"OPTIONAL {?"+p+"m <"+R2RML.inverseExpression+"> ?"+p+"inverseexpression}";
		
		return query;
	}
	
	
	
	private class TermMapQueryResult{
		
		public TermMapQueryResult(QuerySolution sol, String prefix, FromItem fi) {
			template =  sol.get("?"+prefix+"template")!=null?cleanTemplate(sol.get("?"+prefix+"template").asLiteral().getString(),fi):null;
			column = sol.get("?"+prefix+"column")!=null?sol.get("?"+prefix+"column").asLiteral().getString():null;
			column = getRealColumnName(column,fi);
			
			lang = sol.get("?"+prefix+"lang")!=null?sol.get("?"+prefix+"lang").asLiteral().getString():null;
			inverseExpression = sol.get("?"+prefix+"inverseexpression")!=null?sol.get("?"+prefix+"inverseexpression").asLiteral().getString():null;
			constant = sol.get("?"+prefix+"constant");
			datatypeuri = sol.get("?"+prefix+"datatypeuri")!=null?sol.get("?"+prefix+"datatypeuri").asResource():null;
			tmclass = sol.get("?"+prefix+"tmclass")!=null?sol.get("?"+prefix+"tmclass").asResource():null;
			termType = sol.getResource("?termtype");
		}
		
		String[] template;
		String column;
		RDFNode constant;
		String lang;
		Resource datatypeuri;
		String inverseExpression;
		Resource tmclass;
		Resource termType;
		int termTypeInt;
	}
	
	
	






public String getRealColumnName(String unrealColumnName, FromItem fi){
	if(unrealColumnName==null){
		return null;
	}else	if(unrealColumnName.startsWith("\"")&&unrealColumnName.endsWith("\"")){
		return unrealColumnName.substring(1,unrealColumnName.length()-1);
	}else{
		//not escaped, so we need to see how the database handles the string.
		return dbconf.getColumnName(fi, unrealColumnName);
	}
}

public static String unescape(String toUnescape){
	if(toUnescape!=null&&toUnescape.startsWith("\"")&&toUnescape.endsWith("\"")){
		return toUnescape.substring(1,toUnescape.length()-1);
	}else{
		//not escaped, so we need to see how the database handles the string.
		return toUnescape;
	}
}

/**
 * removes all apostrophes from the template and ensure they are correctly capitalized.
 * @return
 */

public String[] cleanTemplate(String template,FromItem fi){
	
	// ((?<!\\\\)\\{)|(\\})
	List<String>  altSeq = Arrays.asList(template.split( "((?<!\\\\)\\{)|(?<!\\\\)\\}"));
	List<String> cleaned=  new ArrayList<String>();
	
	
	
	for (int i = 0; i < altSeq.size(); i++) {
		if (i % 2 == 1) {
			
			
			cleaned.add(getRealColumnName(altSeq.get(i), fi));
			
		} else {
			//static part, no need to change anything, just remove the escape patterns;
			
			
			cleaned.add( altSeq.get(i).replaceAll("\\\\", ""));
			
		}
	}
	

	
	
	
	return cleaned.toArray(new String[0]);
}


public String cleanSql(String toUnescape){
	if(toUnescape!=null){
		
		toUnescape = toUnescape.trim();
		toUnescape = toUnescape.replaceAll("\r\n", " ").replaceAll("\n", " ");
		if(toUnescape.endsWith(";")){
			toUnescape = toUnescape.substring(0, toUnescape.length()-1);
		}
		
		return toUnescape;
	}else{
		return toUnescape;
	}
}


public RDFDatatype getSqlDataType(String tablename, String colname) {
	throw new ImplementationException("implement sql type table");
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
