package sparqlmap;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Properties;

import org.aksw.sparqlmap.SparqlMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * for command line access to sparqlmap
 * dumps 
 * @author joerg
 *
 */
public class dump extends AbstractSparqlMapCommanLineOperation{
	

	
	public dump(PrintStream out, PrintStream err) {
		super(out, err);
	}
	public dump() {
		super(System.out, System.err);
	}
	

	@Override
	public void addCommandspecificOptions(Options options) {
		options.addOption(OptionBuilder.withArgName("r2rml-file").hasArg().isRequired().withDescription("The R2RML file according which defines the mapping for the dump.").create("r2rmlfile"));		
	}
	
	@Override
	public void addCommandspecificProperties(CommandLine cl, Properties props) throws MissingArgumentException {
//		if(!cl.hasOption("r2rml")){
//			throw new MissingArgumentException("Provide an r2rml file using the r2rmlfile option.");
//		}
//		
		props.setProperty("sm.mappingfile", cl.getOptionValue("r2rmlfile"));
		
	}
	
	@Override
	public void doCommandSpecificOperations(
			AnnotationConfigApplicationContext ctxt) {
		
		SparqlMap r2r = ctxt.getBean(SparqlMap.class);
		try {
			r2r.dump(out);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public static void main(String[] args) {
		dump dump = new dump();
		dump.processCommand(args);
		
	}

	@Override
	public String getCommandName() {
		return "sparqlmap.dump";
	}
	
	

}