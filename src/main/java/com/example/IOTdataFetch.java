package com.example;

import com.dao.CitiesData;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.datastore.v1.Query;

public class IOTdataFetch {

	private static long row_id = 0;
	static final DoFn<Entity, String> MUTATION_TRANSFORM = new DoFn<Entity, String>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void processElement(DoFn<Entity, String>.ProcessContext context) throws Exception {
			Entity data = context.element();
			
			context.output(data.toString());
		}

	}; 
	public static void main(String[] args) 
	{
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		options.setStagingLocation("gs://mihin-data/staging12");
		Pipeline p = Pipeline.create(options);
		
		Query query;
//		p.apply(DatastoreIO.v1().read().withProjectId("healthcare-12").withQuery( query)).apply(ParDo.named("Processing File").of(MUTATION_TRANSFORM)).apply(TextIO.Write.named("Writeing to File")
//				.to("healthcare-12:health_care_data.500_cities_local_data_for_better_health2"));
//		p.run();

	}
}
