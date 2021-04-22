package LandingZoneToFormattedZone;

import org.json.JSONArray;

public class ParallelPartialQueryGenerator {
	private ParallelWorker[] partialQ;
	private int numThreads;
	
	public ParallelPartialQueryGenerator(int numThreads){
		this.numThreads = numThreads;
		this.partialQ = new ParallelWorker[numThreads];
	}
	
	
	public String partialQueriesSum(JSONArray contentArray, String whichData) throws InterruptedException {
		
		int steps = (int) Math.ceil((contentArray.length()*1.0)/numThreads);
		
		for(int i = 0; i < numThreads; ++i) {
			partialQ[i] = new ParallelWorker(contentArray, i*steps, Math.min((i+1)*steps, contentArray.length()), whichData);
			partialQ[i].start();
		}
		
		for(ParallelWorker worker: partialQ) {
			worker.join();
		}
		
		String resultQuery = "";
		for(ParallelWorker worker: partialQ) {
			resultQuery += worker.getPartialQuery();
		}
		
		return resultQuery;
	}
}
