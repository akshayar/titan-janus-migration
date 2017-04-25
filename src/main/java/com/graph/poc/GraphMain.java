package com.graph.poc;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.graph.poc.db.GraphDBInitializer;

@SpringBootApplication
public class GraphMain {
	private static final Logger logger = Logger.getLogger(GraphMain.class);

	public GraphMain() {
		logger.info("Constructor");
	}

	public static void main(String[] args) {

		if(args.length<2){
			startGremlinNCrud(args);	
		}else{
			startGremlinServer(args);
		}
		

	}

	private static void startGremlinNCrud(String[] args) {
		startGremlinServer(args);
		SpringApplication.run(GraphMain.class, args);
	}

	private static void startGremlinServer(String[] args) {
		try {
			if (args.length != 0) {
				logger.info("Starting germlin server");
				GremlinServerModified custom = GremlinServerModified.start(args);
				Map<String, Graph> graphs = custom.getServerGremlinExecutor().getGraphManager().getGraphs();
				Graph graph = graphs.values().iterator().next();
				GraphDBInitializer.setGraph(graph);
				logger.info("Done -Starting germlin server");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
