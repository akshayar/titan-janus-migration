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

		if (args.length < 2) {
			startGremlinNCrud(args);
		} else {
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
				Graph graph = startGremlinNGetGraph(args);
				GraphDBInitializer.setGraph(graph);

				forceConfigChange(graph);
				
				close(graph);

				graph = startGremlinNGetGraph(args);
				GraphDBInitializer.setGraph(graph);

				logger.info("Done -Starting germlin server");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void forceConfigChange(Graph graph) {
		GraphForceIndexNameStrategy strategy = new GraphForceIndexNameStrategy(graph);
		strategy.forceIndexConfiguration();
	}

	private static Graph startGremlinNGetGraph(String[] args) throws Exception {
		logger.info("Starting germlin server");
		GremlinServerModified custom = GremlinServerModified.start(args);
		Map<String, Graph> graphs = custom.getServerGremlinExecutor().getGraphManager().getGraphs();
		Graph graph = graphs.values().iterator().next();
		return graph;
	}
	
	private static void close(Graph graph){
		try {
			graph.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

}
