package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.util.Iterator;

import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import org.aksw.sparqlmap.beautifier.SparqlBeautifier;
import org.aksw.sparqlmap.config.syntax.DBConnectionConfiguration;
import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.mapper.Mapper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml.MappingFilterFinder;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;

public class AlgebraBasedMapper implements Mapper {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AlgebraBasedMapper.class);

	private R2RMLModel mappingConf;

	private DBConnectionConfiguration dbconf;
	
	private SparqlBeautifier beautifier = new SparqlBeautifier();
	
	public SparqlBeautifier getBeautifier() {
		return beautifier;
	}


	
	public AlgebraBasedMapper(R2RMLModel mappingConf, DBConnectionConfiguration dbconf){
		this.mappingConf = mappingConf;
		this.dbconf = dbconf;
	}




	public String rewrite(Query sparql) {
		
		Query origQuery = sparql;

		log.debug(origQuery.toString());
		//first we beautify the Query


		Op op = this.beautifier.compileToBeauty(origQuery); // new  AlgebraGenerator().compile(beautified);
		log.debug(op.toString());
		
		MappingFilterFinder mff = new MappingFilterFinder(mappingConf, op);
		

		
		QueryBuilderVisitor builderVisitor = new QueryBuilderVisitor(mappingConf, mff,dbconf.getDataTypeHelper());
		
		
		OpWalker.walk(op, builderVisitor);
		
		
		// prepare deparse select
		StringBuffer out = new StringBuffer();
		Select select = builderVisitor.getSqlQuery();
		SelectDeParser selectDeParser  = dbconf.getSelectDeParser(out);
		
		selectDeParser.setBuffer(out);
//		ExpressionDeParser expressionDeParser =  mappingConf.getR2rconf().getDbConn().getExpressionDeParser(selectDeParser, out);
//		selectDeParser.setExpressionVisitor(expressionDeParser);
		if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
			out.append("WITH ");
			for (Iterator iter = select.getWithItemsList().iterator(); iter.hasNext();) {
				WithItem withItem = (WithItem)iter.next();
				out.append(withItem);
				if (iter.hasNext())
					out.append(",");
				out.append(" ");
			}
		}
		select.getSelectBody().accept(selectDeParser);
		
		
		
		String sqlResult = out.toString();
		log.debug(sqlResult);
		return sqlResult;
		
	

	}

}
