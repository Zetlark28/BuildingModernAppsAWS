package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.JSONInput;
import com.amazonaws.services.s3.model.JSONOutput;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentEvent.EndEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEvent.StatsEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;
import com.amazonaws.util.IOUtils;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class App implements
    RequestHandler<Map<String, String>, String> {

  private static final AWSSimpleSystemsManagement ssm = AWSSimpleSystemsManagementClientBuilder.defaultClient();
  private static final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

  @Override
  public String handleRequest(Map<String, String> event,
      Context context) {

    readDragonData(event);
    return "Dragon validate";
  }

  private static void readDragonData(Map<String, String> event) {
    String bucketName = getBucketName();
    String key = getKey();
    String query = getQuery(event);
    SelectObjectContentRequest request = generateJSONRequest(bucketName, key, query);
    final AtomicBoolean isResultComplete = new AtomicBoolean(false);

    SelectObjectContentResult result = s3Client.selectObjectContent(request);
    InputStream resultInputStream = result.getPayload().getRecordsInputStream(
        new SelectObjectContentEventVisitor() {
          @Override
          public void visit(StatsEvent event) {
            System.out.println(
                "Received Stats, Bytes Scanned: " + event.getDetails().getBytesScanned()
                    + " Bytes Processed: " + event.getDetails().getBytesProcessed()
            );

          }

          @Override
          public void visit(EndEvent event) {
            isResultComplete.set(true);
          }
        }
    );

    String text = null;

    try {
      text = IOUtils.toString(resultInputStream);
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }

    if(text != null && !(text.equals(""))){
      throw new DragonValidationException("Duplicate dragon reported", new RuntimeException());
    }

  }

  private static SelectObjectContentRequest generateJSONRequest(String bucketName, String key,
      String query) {
    SelectObjectContentRequest request = new SelectObjectContentRequest();
    request.setBucketName(bucketName);
    request.setKey(key);
    request.setExpression(query);
    request.setExpressionType(ExpressionType.SQL);

    InputSerialization inputSerialization = new InputSerialization();
    inputSerialization.setJson(new JSONInput().withType("Document"));
    inputSerialization.setCompressionType(CompressionType.NONE);
    request.setInputSerialization(inputSerialization);

    OutputSerialization outputSerialization = new OutputSerialization();
    outputSerialization.setJson(new JSONOutput());
    request.setOutputSerialization(outputSerialization);

    return request;
  }

  private static String getQuery(Map<String, String> queryParams) {

    return "select * from S3Object[*][*] s where s.dragon_name_str='" + queryParams.get(
        "dragonName") + "'";

  }


  private static String getBucketName() {
    GetParameterRequest bucketParameterRequest = new GetParameterRequest().withName(
        "dragon_data_bucket_name").withWithDecryption(false);
    GetParameterResult bucketResult = ssm.getParameter(
        bucketParameterRequest);
    return bucketResult.getParameter().getValue();
  }


  private static String getKey() {
    GetParameterRequest keyParameterRequest = new GetParameterRequest().withName(
        "dragon_data_file_name").withWithDecryption(false);
    GetParameterResult keyResult = ssm.getParameter(
        keyParameterRequest);
    return keyResult.getParameter().getValue();
  }


}
