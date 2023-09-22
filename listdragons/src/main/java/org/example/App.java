package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.JSONInput;
import com.amazonaws.services.s3.model.JSONOutput;
import com.amazonaws.services.s3.model.OutputSerialization;
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
import java.util.HashMap;
import java.util.Map;

public class App implements
    RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

  private static final AWSSimpleSystemsManagement ssm = AWSSimpleSystemsManagementClientBuilder.defaultClient();
  private static final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

  public APIGatewayProxyResponseEvent handleRequest(
      APIGatewayProxyRequestEvent event, Context context) {

    String dragonData = readDragonData(event);
    return generateResponse(dragonData);
  }

  private APIGatewayProxyResponseEvent generateResponse(String dragonData) {
    APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
    response.setStatusCode(200);
    response.setBody(new Gson().toJson(dragonData));
    final Map<String, String> headers = new HashMap<>();
    headers.put("Access-Control-Allow-Origin", "*");
    headers.put("Content-Type", "application/json");
    response.setHeaders(headers);

    return response;
  }

  private static String readDragonData(APIGatewayProxyRequestEvent event) {
    Map<String, String> queryParams = event.getQueryStringParameters();
    String bucketName = getBucketName();
    String key = getKey();
    String query = getQuery(queryParams);
    SelectObjectContentRequest request = generateJSONRequest(bucketName, key, query);
    return queryS3(request);
  }

  private static String queryS3(SelectObjectContentRequest request) {
    SelectObjectContentResult result = s3Client.selectObjectContent(
        request);

    InputStream resultInputStream = result.getPayload().getRecordsInputStream(
        new SelectObjectContentEventVisitor() {
          @Override
          public void visit(StatsEvent event) {
            System.out.println(
                "Received Stats, Bytes Scanned: " + event.getDetails().getBytesScanned()
                    + " Bytes Processed: " + event.getDetails().getBytesProcessed()
            );

          }

        }
    );

    String text = null;

    try {
      text = IOUtils.toString(resultInputStream);
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }

    return text;
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
    if (queryParams != null) {
      if (queryParams.containsKey("family")) {
        return "select * from S3Object[*][*] s where s.family_str='" + queryParams.get("family")
            + "'";
      } else if (queryParams.containsKey("dragonName")) {
        return "select * from S3Object[*][*] s where s.dragon_name_str='" + queryParams.get(
            "dragonName") + "'";

      }
    }
    return "select * from s3object s";
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
