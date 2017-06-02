package com.example;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.util.HashMap;
public class MedicationOrderEntry
{
	static final DoFn<String, TableRow> MUTATION_TRANSFORM = new DoFn<String, TableRow>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
			String line = c.element();
			String patient_id;
			String medicationOrder_id;
			String medicationCodeableConcept_id;
			String medicationCodeableConcept_text = null;
			String practitioner_id;
			String encounter_id;
			JSONParser parser = new JSONParser();
			try {
				Object object = parser.parse(line);
				JSONObject jsonObject = (JSONObject) object;
				JSONArray resource = (JSONArray) jsonObject.get("resources"); 
				//System.out.println(resource);
				//
				for (int i = 0; i < resource.size() ; i++) {
					JSONObject jsonObject1 = (JSONObject) parser.parse(resource.get(i).toString());
					HashMap<String, String> map2   =  (HashMap<String, String>) jsonObject1.get("resource");
					String medicationId = map2.get("id").toString();
					HashMap<String, JSONObject> map   =  (HashMap<String, JSONObject>) jsonObject1.get("resource");
					//					System.out.println(map);
					medicationOrder_id = (medicationId);
					HashMap<String, String> encounterObject = (JSONObject) (map.get("encounter"));
					String encounterReference = (encounterObject.get("reference"));
					encounter_id = encounterReference.substring(encounterReference.indexOf('/')+1);
					if(! map.containsKey("prescriber")){
						continue;
					}
					HashMap<String, String> prescriberObject = (JSONObject) (map.get("prescriber"));
					String prescriberReference = (prescriberObject.get("reference"));
					String prescriber_id = prescriberReference.substring(prescriberReference.indexOf('/')+1);
					practitioner_id = (prescriber_id);
					HashMap<String, String> patientObject = (JSONObject) (map.get("patient"));
					String patientReference = (patientObject.get("reference"));
					patient_id = patientReference.substring(patientReference.indexOf('/')+1);

					if(!map.containsKey("medicationCodeableConcept")){
						continue;
					}
					HashMap<String, String> medicationObject = (JSONObject) (map.get("medicationCodeableConcept"));
					if (medicationObject.containsKey("text")) {
						String medication = (medicationObject.get("text"));
						medicationCodeableConcept_text = medication.substring(medication.indexOf(':')+1);
					}

					HashMap<String, JSONArray> medicationObject2 =  (map.get("medicationCodeableConcept"));
					JSONArray coding  = (medicationObject2.get("coding"));

					JSONObject codingObject  = (JSONObject) parser.parse((coding.get(0)).toString());
					String medicationCode = codingObject.get("code").toString();
					medicationCodeableConcept_id = (medicationCode);
					TableRow row = new TableRow().set("encounter_id", encounter_id).set("medicationOrder_id", medicationOrder_id).set("patient_id",patient_id).set("practitioner_id",practitioner_id)
							.set("medicationCodeableConcept_id",medicationCodeableConcept_id).set("medicationCodeableConcept_text",medicationCodeableConcept_text);
					c.output(row);
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}

	}; 
	public static void main(String[] args) 
	{
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		options.setStagingLocation("gs://mihin-data/staging12");
		Pipeline p = Pipeline.create(options);
		p.apply(TextIO.Read.from("gs://mihin-data/formatedMedicationOrder_entry.json")).apply(ParDo.named("Applying Transformations").of(MUTATION_TRANSFORM))
		.apply(BigQueryIO.Write
				.named("Write")
				.to("healthcare-12:Mihin_Data_Sample.MedicationOrderEntry")
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		p.run();

	}

}
