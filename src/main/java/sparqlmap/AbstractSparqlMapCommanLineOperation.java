package sparqlmap;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Properties;

import org.aksw.sparqlmap.SparqlMap;
import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLValidationException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.springframework.beans.BeansException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;

public abstract class AbstractSparqlMapCommanLineOperation {

	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(AbstractSparqlMapCommanLineOperation.class);

	public AbstractSparqlMapCommanLineOperation(PrintStream out, PrintStream err) {
		this.out = out;
		this.err = err;
	}

	PrintStream out;
	PrintStream err;

	public SparqlMap sparqlMap;
	private Options options;

	public AnnotationConfigApplicationContext setupSparqlMap(
			Properties... props) throws Throwable {

		AnnotationConfigApplicationContext ctxt;

		ctxt = new AnnotationConfigApplicationContext();

		for (int i = 0; i < props.length; i++) {
			ctxt.getEnvironment()
					.getPropertySources()
					.addFirst(
							new PropertiesPropertySource("props " + i, props[i]));
		}

		ctxt.scan("org.aksw.sparqlmap");
		ctxt.refresh();
		return ctxt;

	}

	private Throwable getExceptionCause(BeansException be) {
		if (be.getCause() instanceof BeansException) {
			return getExceptionCause((BeansException) be.getCause());
		} else {
			return be.getCause();
		}
	}

	public static Options getOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder
				.withArgName("db-file")
				.hasArg()
				.withDescription(
						"A file properties file containing the parameters for connecting to the database.")
				.create("dbfile"));
		options.addOption(OptionBuilder
				.withArgName("jdbc-url")
				.hasArg()
				.withDescription(
						"the full connection url for the database, like it is used for connection in java to a db. Example: jdbc:postgresql://localhost/mydb")
				.create("dburi"));
		options.addOption(OptionBuilder.withArgName("db-username").hasArg()
				.withDescription("username for the db connection")
				.create("dbuser"));
		options.addOption(OptionBuilder.withArgName("db-password").hasArg()
				.withDescription("password for the db connection")
				.create("dbpass"));
		options.addOption(OptionBuilder
				.withArgName("base-iri")
				.hasArg()
				.withDescription(
						"Base iri used for cases in the mapping process, where no explicit iri is defined.")
				.create("baseiri"));
		return options;
	}

	public void processCommand(String[] args) {
		try {
			CommandLineParser clparser = new PosixParser();
			// get the default
			options = getOptions();
			// get the command specific
			addCommandspecificOptions(options);

			// process the commandline args
			CommandLine cl = null;

			cl = clparser.parse(options, args);

			// process the basic options
			Properties props = new Properties();
			if (cl.hasOption("baseiri")) {
				props.setProperty("sm.baseuri", cl.getOptionValue("baseiri"));
			} else {
				props.setProperty("sm.baseuri", "http://localhost/baseuri/");
			}

			// process the database options
			processDbOptions(options, cl, props);

			// process the command specific options
			addCommandspecificProperties(cl, props);

			// create a SparqlMap instance out of this
			doCommandSpecificOperations(setupSparqlMap(props));

		} catch (ParseException e) {
			error(e.getMessage());
		} catch (FileNotFoundException e) {
			error("File not found: " + e.getMessage());
		} catch (IOException e) {
			error("Error reading file: " + e.getMessage());
		} catch (R2RMLValidationException e) {
			error("Error validation the mapping file: " + e.getMessage());
		} catch (BeansException e) {
			err.println("Error during setup: "
					+ getExceptionCause(e).getMessage());

		} catch (Throwable t) {
			log.error("Another error happened: ", t);
			err.println("Error setting up the app: " + t.getMessage());

		}

	}

	private void processDbOptions(Options options, CommandLine cl,
			Properties props) throws MissingOptionException,
			FileNotFoundException, IOException {
		if (cl.hasOption("dbfile")) {
			// load db file

			props.load(new FileReader(cl.getOptionValue("dbfile")));

		} else if (cl.hasOption("dbuser") && cl.hasOption("dbpass")
				&& cl.hasOption("dburi")) {
			//

			props.setProperty("jdbc.url", cl.getOptionValue("dburi"));
			props.setProperty("jdbc.username", cl.getOptionValue("dbuser"));
			props.setProperty("jdbc.password", cl.getOptionValue("dbpass"));
		} else {
			throw new MissingOptionException(
					"Supply the db connection information by either supplying a file or the db options");
		}

		props.setProperty("jdbc.poolminconnections", "5");
		props.setProperty("jdbc.poolmaxconnections", "10");
	}

	public void error(String message) {
		HelpFormatter formatter = new HelpFormatter();
		err.println(message);
		PrintWriter pw = new PrintWriter(out);
		formatter.printHelp(pw, 60,
				"java -jar <pathtojar> " + getCommandName(), "", options, 3, 5,
				"");
		pw.flush();
	}

	public abstract void addCommandspecificOptions(Options options);

	public abstract void addCommandspecificProperties(CommandLine cl,
			Properties props) throws ParseException;

	public abstract void doCommandSpecificOperations(
			AnnotationConfigApplicationContext sparqlmapContext);

	public abstract String getCommandName();

}
