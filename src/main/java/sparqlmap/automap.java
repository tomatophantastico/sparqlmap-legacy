package sparqlmap;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Properties;

import org.aksw.sparqlmap.core.automapper.AutomapperWrapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class automap extends AbstractSparqlMapCommanLineOperation {
	


	public automap(PrintStream out, PrintStream err) {
		super(out, err);
	}
	public automap() {
		super(System.out, System.err);
	}

	@Override
	public void addCommandspecificOptions(Options options) {

	}

	@Override
	public void addCommandspecificProperties(CommandLine cl, Properties props) {

	}

	@Override
	public void doCommandSpecificOperations(
			AnnotationConfigApplicationContext ctxt) {
		AutomapperWrapper am = ctxt.getBean(AutomapperWrapper.class);
		try {
			am.automap().write(out, "TURTLE");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		


	}

	@Override
	public String getCommandName() {
		return "sparqlmap.automap";
	}

}
