package HBaseToWIMEDSDB;

import org.json.JSONArray;

public class ParallelPartialQueryGenerator {
	private ParallelWorker[] partialQ;
	private int numThreads;
	private boolean isUpdate;
	
	public ParallelPartialQueryGenerator(int numThreads, boolean isUpdate){
		this.numThreads = numThreads;
		this.isUpdate = isUpdate;
		this.partialQ = new ParallelWorker[numThreads];
	}
	
	
	public String partialQueriesSum(JSONArray AdminUnit) throws InterruptedException {
		
		System.out.println("...generating SQL queries from HBase data...");
		int steps = (int) Math.ceil((AdminUnit.length()*1.0)/numThreads);
		
		for(int i = 0; i < numThreads; ++i) {
			partialQ[i] = new ParallelWorker(AdminUnit, i*steps, Math.min((i+1)*steps, AdminUnit.length()), isUpdate);
			partialQ[i].start();
		}
		//waiting all workers to finish
		for(ParallelWorker worker: partialQ) {
			worker.join();
		}
		
		String resultQuery = "";
		//adding partial queries into a resultQuery
		for(ParallelWorker worker: partialQ) {
			resultQuery += worker.getPartialQuery();
		}
		
		return resultQuery;
	}
}
