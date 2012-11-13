package org.aksw.sparqlmap.config.syntax.r2rml;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectBodyString;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.columnanalyze.CompatibilityChecker;
import org.aksw.sparqlmap.columnanalyze.CompatibilityCheckerFactory;
import org.aksw.sparqlmap.config.syntax.IDBAccess;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.update.GraphStoreFactory;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.vocabulary.RDF;

public class R2RMLModel {
	
	
	public R2RMLModel(ColumnHelper columnhelper, IDBAccess dbconf,
			DataTypeHelper dth, Model mapping, Model r2rmlSchema) {
		super();
		this.columnhelper = columnhelper;
		this.dbconf = dbconf;
		this.dth = dth;
		this.mapping = mapping;
		this.r2rmlSchema = r2rmlSchema;
	}


	private ColumnHelper columnhelper;
	private IDBAccess dbconf;
	private DataTypeHelper dth;
	Model mapping = null;
	Model r2rmlSchema = null;

	

	
	Map<String,Map<String,String>> col2castTo = new HashMap<String, Map<String, String>>();
	
	
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(R2RMLModel.class);

	Model reasoningModel = null;
	Map<String,TripleMap> tripleMaps = null;
	
	@PostConstruct
	public  void setup() throws R2RMLValidationException, JSQLParserException, SQLException{
		reasoningModel = ModelFactory.createRDFSModel(r2rmlSchema,mapping);	
		resolveRRClassStatements();
		resolveR2RMLShortcuts();
		loadTripleMaps();
		loadParentTripleStatements();
		
	
		
		loadCompatibilityChecker();
	}
	
	
	private void loadParentTripleStatements() {
		String ptquery = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> \n" + 
				"SELECT DISTINCT * \n" + 
				"{\n" + 
				"?tmuri rr:predicateObjectMap ?refPredObjMap. \n" + 
				" ?refPredObjMap rr:objectMap ?refObjectMap.\n" +
				" ?refPredObjMap rr:predicateMap ?pm.\n" +
				getTermMapQuery("p") + 
				"?refObjectMap a rr:RefObjectMap.\n" + 
				"?refObjMap rr:parentTriplesMap ?parentTmUri. \n" +  
				"\n" + 
				"}";
		log.info(ptquery);

		
		
		ResultSet tmrs = QueryExecutionFactory.create(QueryFactory.create(ptquery), reasoningModel).execSelect();
		
		while(tmrs.hasNext()){
			
			QuerySolution sol = tmrs.next();
			
			String tmUri = sol.get("tmuri").asResource().getURI();
			TripleMap tm  = this.tripleMaps.get(tmUri);
			String parentTripleMapUri = sol.get("parentTmUri").asResource().getURI();
			TripleMap parentTm = this.tripleMaps.get(parentTripleMapUri);
			
			TermMap newTermMap = parentTm.getSubject().clone("");
			
			

			
			//get join conditions
			
			String joinConditionQuery = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> \n" + 
					"SELECT * \n" + 
					"{\n" + 
					"<"+tmUri+"> rr:predicateObjectMap ?refPredObjMap. \n" + 
					" ?refPredObjMap rr:objectMap ?refObjectMap.\n" + 
					"?refObjectMap a rr:RefObjectMap.\n" + 
					"?refObjMap rr:parentTriplesMap <"+parentTripleMapUri+ ">. \n" + 				
					"?refObjMap rr:joinCondition ?jc. ?jc rr:child ?jcchild. ?jc rr:parent ?jcparent" + 
					"\n" + 
					"}";
			
			ResultSet jcrs = QueryExecutionFactory.create(QueryFactory.create(joinConditionQuery), reasoningModel).execSelect();
			
			while(jcrs.hasNext()){
				
				QuerySolution jcsol = jcrs.next();
				
				String parentjcColName = jcsol.get("jcparent").asLiteral().toString();
				//validate it
				this.dbconf.getDataType(parentTm.from, getRealColumnName(parentjcColName,parentTm.from));
				
				Column leftCol = new Column(new Table(null, parentTm.from.getAlias()), parentjcColName);

				String childjcColName = jcsol.get("jcchild").asLiteral().toString();
				
				Column rightCol = new Column(new Table(null,tm.from.getAlias()),childjcColName);
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(dth.cast(leftCol, dth.getStringCastType()));
				eq.setRightExpression(dth.cast(rightCol, dth.getStringCastType()));
				newTermMap.getFromJoins().add(eq);
				newTermMap.addFromItem(tm.from);	
			}
			
			
			//now we need to get the predicate	
			
//			String pquery = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> \n" + 
//					"SELECT * \n" + 
//					"{\n" + 
//			" ?refPredObjMap rr:predicateMap ?refPredicateMap.\n" + 
//			getTermMapQuery("p") + " FILTER (?refPredicateMap = <" + sol.getResource("refPredicateMap").getURI() + ">)}";
//			
//			
//			ResultSet prs = QueryExecutionFactory.create(QueryFactory.create(pquery), reasoningModel).execSelect();
//			
			
			TermMap ptm = null;
		
				TermMapQueryResult ptmrs = new TermMapQueryResult(sol,"p",tm.from);
				
				
				
				//some general validation
				if(ptmrs.termType!=null&&!ptmrs.termType.getURI().equals(R2RML.IRI)){
					throw new R2RMLValidationException("Only use iris in predicate position");
				}

				ptm = createTermMap(tm.from, ResourceFactory.createResource(tmUri), tm, null, ptmrs, ColumnHelper.COL_VAL_TYPE_RESOURCE);
				
			
		
			tm.addPO(ptm, newTermMap);
			
			
		
			
			
			
			
			
		}
		
		
		
		
		
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


	private void loadCompatibilityChecker() throws SQLException {
		CompatibilityCheckerFactory ccfac = new CompatibilityCheckerFactory(reasoningModel,dbconf);
		
		for (TripleMap tripleMap : tripleMaps.values()) {
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

		
		return new HashSet(tripleMaps.values());
	}
	
	

	
	
	private  void loadTripleMaps() throws R2RMLValidationException, JSQLParserException{
		Map<String,TripleMap> tripleMaps = new HashMap<String,TripleMap>();
		
	
		String tmquery = "PREFIX rr: <http://www.w3.org/ns/r2rml#> SELECT ?tm ?tableName ?query ?version {?tm a rr:TriplesMap. ?tm rr:logicalTable ?tab . {?tab rr:tableName ?tableName} UNION {?tab rr:sqlQuery ?query. OPTIONAL{?tab rr:sqlVersion ?version}}}" ;
		ResultSet tmrs = QueryExecutionFactory.create(QueryFactory.create(tmquery), reasoningModel).execSelect();
		
		while(tmrs.hasNext()){
		
			
			QuerySolution solution = tmrs.next();
			Resource tmUri= solution.get("tm").asResource();
			String tablename = solution.get("?tableName")!=null?solution.get("?tableName").asLiteral().getString():null;
			String query = solution.get("?query")!=null?solution.get("?query").asLiteral().getString():null;
			Resource version  = solution.get("?version")!=null?solution.get("?version").asResource():null;
			
			
			
			
			
			FromItem fromItem;
			Table fromTable;
			SubSelect subsel;
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
				subsel.setSelectBody(new SelectBodyString(query));
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
			
			//get the s-graph
			String sgraphquery  = "PREFIX rr: <http://www.w3.org/ns/r2rml#> SELECT  * {" +
					" <"+tmUri.getURI()+"> rr:subjectMap ?sm. " +
							"?sm rr:graphMap ?gm. " +
							"OPTIONAL { ?gm rr:template ?template } "+
							"OPTIONAL { ?gm rr:column ?column } " +
							"OPTIONAL { ?gm rr:constant ?constant } }";
			log.info(sgraphquery);
			ResultSet sgraphrs = QueryExecutionFactory.create(QueryFactory.create(sgraphquery), this.mapping).execSelect();
			Expression graph = null;
			while(sgraphrs.hasNext()){
				QuerySolution graphqs = sgraphrs.next();
				if(graphqs.get("?template")!=null){
					String template = graphqs.get("?template").asLiteral().toString();
					graph = columnhelper.getGraphExpression(cleanTemplate(template, fromItem), fromItem,dth);
				} else if(graphqs.get("?column")!=null){
					String template = "\"{" +getRealColumnName(graphqs.get("?column").asLiteral().toString(), fromItem) + "\"}";
					graph = columnhelper.getGraphExpression(cleanTemplate(template, fromItem), fromItem,dth);
				} else if(graphqs.get("?constant")!=null){
					RDFNode constant = graphqs.get("?constant");
					graph = columnhelper.getGraphExpression(constant, dth);
					
				}
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
			//some validation
			if(sres.termType==null||sres.termType.hasURI(R2RML.Literal)){
				new R2RMLValidationException("no literal in subject position");
			}
			if(sres.constant!=null&& !sres.constant.isURIResource()){
					throw new R2RMLValidationException("Must IRI in predicate position");
				
			}
			
			
			
			
		
			
			triplemap.subject = createTermMap(fromItem,  tmUri, triplemap, graph, sres, sres.termTypeInt);
			
			
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
	
				ptm = createTermMap(fromItem, tmUri, triplemap, graph, p, ColumnHelper.COL_VAL_TYPE_RESOURCE);
				
				
				TermMapQueryResult o = new TermMapQueryResult(posol,"o",fromItem);
				o.termTypeInt = ColumnHelper.COL_VAL_TYPE_RESOURCE;
				
				TermMap otm=  null;
				
				if(o.column!=null||o.lang!=null||o.datatypeuri !=null||(o.termType!=null&&o.termType.getURI().equals(R2RML.Literal))){
					o.termTypeInt = ColumnHelper.COL_VAL_TYPE_LITERAL;
				}else if(o.termType!=null&&o.termType.getURI().equals(R2RML.BlankNode)){
					o.termTypeInt = ColumnHelper.COL_VAL_TYPE_BLANK;
				}

				otm = createTermMap(fromItem, tmUri, triplemap,
						graph, o, o.termTypeInt);
				
				
				triplemap.addPO(ptm, otm);

			}
			
			tripleMaps.put(tmUri.toString(),triplemap);
			
			
		}
		
	
		
		this.tripleMaps =  tripleMaps;
	}


	private TermMap createTermMap(FromItem fromItem,
			Resource tmUri, TripleMap triplemap, Expression graph,
			TermMapQueryResult tmqrs, Integer otermtype) {
		TermMap tm = null;
		String datatype = tmqrs.datatypeuri!=null?tmqrs.datatypeuri.getURI():null;
		if(tmqrs.column!=null){
			// generate from colum
			Column col = new Column();
			if(fromItem instanceof Table){
				col.setTable((Table) fromItem);
			}else{
				Table tab = new Table("null",fromItem.getAlias());
				tab.setAlias(fromItem.getAlias());
				col.setTable(tab);
			}
			
			col.setColumnName(tmqrs.column);
			List<Expression> oexprs = columnhelper.getExpression(col,otermtype,dbconf.getDataType(fromItem, tmqrs.column), datatype, tmqrs.lang, null, dth,graph,tmUri.getNameSpace());
			tm = new TermMap(dth, oexprs, Arrays.asList(fromItem), null, triplemap);
		}else if(tmqrs.constant != null){
			//use constant term

			List<Expression> oexprs = columnhelper.getExpression(tmqrs.constant, dth,graph);
			tm = new TermMap(dth, oexprs, Arrays.asList(fromItem), null, triplemap);
		}else if(tmqrs.template!=null){
			int sqlType = ColumnHelper.COL_VAL_SQL_TYPE_RESOURCE;
			if(otermtype!=ColumnHelper.COL_VAL_RES_LENGTH_LITERAL){
				sqlType = Types.VARCHAR;
			}
			
			
			//from template
			List<Expression> otmExpressions = columnhelper.getExpression(tmqrs.template, otermtype, sqlType,datatype, tmqrs.lang,null, dth,fromItem,graph,tmUri.getNameSpace());
			tm = new TermMap(dth,otmExpressions,Arrays.asList(fromItem), null,triplemap);
		}
		return tm;
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
			datatypeuri = sol.get("?"+prefix+"datatype")!=null?sol.get("?"+prefix+"datatype").asResource():null;
			tmclass = sol.get("?"+prefix+"tmclass")!=null?sol.get("?"+prefix+"tmclass").asResource():null;
			termType = sol.getResource("?"+prefix+ "termtype");
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
