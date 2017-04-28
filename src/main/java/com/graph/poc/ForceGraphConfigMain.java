package com.graph.poc;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraph;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ForceGraphConfigMain {
	private static final Logger logger = Logger.getLogger(ForceGraphConfigMain.class);

	public ForceGraphConfigMain() {
		logger.info("Constructor");
	}

	public static void main(String[] args) throws Exception {

		Graph graph = startGremlinNGetGraph(args);
		forceConfigChange(graph);
		close(graph);
		logger.info("Graph closed ?-"+((JanusGraph)graph).isClosed());
		System.exit(0);
		
		

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
