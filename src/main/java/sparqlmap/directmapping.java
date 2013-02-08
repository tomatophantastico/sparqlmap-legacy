package sparqlmap;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Properties;

import org.aksw.sparqlmap.SparqlMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class directmapping extends AbstractSparqlMapCommanLineOperation {
	
	
	public directmapping(PrintStream out, PrintStream err) {
		super(out, err);
	}
	public directmapping() {
		super(System.out, System.err);
	}

	@Override
	public void addCommandspecificOptions(Options options) {
		options.addOption(OptionBuilder.withArgName("dump-file").hasArg().withDescription("Dumps a R2RML representation of the direct mapping into the specified file.").create("r2rmldump"));
	}
	
	@Override
	public void addCommandspecificProperties(CommandLine cl, Properties props) {
		if(cl.hasOption("r2rmldump")){
			props.setProperty("sm.dmr2rmldump",cl.getOptionValue("r2rmldump"));
		}
		
		
	}
	
@Override
	public void doCommandSpecificOperations(
			AnnotationConfigApplicationContext ctxt) {
		SparqlMap r2r = ctxt.getBean(SparqlMap.class);
		try {
			r2r.dump(out);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		ctxt.close();
		
	}
	
	public static void main(String[] args) throws SQLException, ParseException {
		
		directmapping dm = new directmapping();
		dm.processCommand(args);

	}

	@Override
	public String getCommandName() {
		return "sparqlmap.directmapping";
	}
	
	

}
