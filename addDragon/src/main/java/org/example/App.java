package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class App implements
    RequestHandler<Dragon, String> {

  private static final AWSSimpleSystemsManagement ssm = AWSSimpleSystemsManagementClientBuilder.defaultClient();
  private static final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();


  @Override
  public String handleRequest(Dragon event, Context context) {
    addDragon(event);
    return "Dragon added";
  }

  private static void addDragon(Dragon event) {
    S3Object object = s3Client.getObject(new GetObjectRequest(getBucketName(), getKey()));
    InputStream objectInputStream = object.getObjectContent();
    String dragonDataString = convertTextInputStreamToString(objectInputStream);

    List<Dragon> dragonDataList = convertStringToList(dragonDataString);

    addNewDragonToList(dragonDataList, event);

    uploadObjectToS3(getBucketName(), getKey(), dragonDataList);
  }

  private static void uploadObjectToS3(String bucketName, String key, List<Dragon> dragonDataList) {
    s3Client.putObject(bucketName, key, gson.toJson(dragonDataList));
  }

  private static void addNewDragonToList(List<Dragon> dragons, Dragon dragon) {
    dragons.add(dragon);
  }

  private static List<Dragon> convertStringToList(String dragonData) {
    return gson.fromJson(dragonData, new TypeToken<List<Dragon>>() {
    }.getType());
  }

  private static String convertTextInputStreamToString(InputStream input) {
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    String line = null;
    StringBuilder objectContent = new StringBuilder();

    try {
      while ((line = reader.readLine()) != null) {
        objectContent.append(line);
      }
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }

    return objectContent.toString();
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
