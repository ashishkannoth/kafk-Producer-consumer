package com.kafka.controller;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.kafka.model.User;

@Service
public class Consumer {

	private Regions clientRegion = Regions.AP_SOUTH_1;

	private String bucketName = "myfirstbucket75";

	private String sec_key = "EwuMZwVb1NpWdixOIw5YiVymVx28fBypIc+QsXCF";

	private String access_key = "AKIAUXKSEV7PIMSFG6MV";

	@KafkaListener(topics = "kafka_message", groupId = "group_id")
	public void consumedMessage(String message) {
		System.out.println("Ther message is = " + message);
	}

	@KafkaListener(topics = "kafka_message", groupId = "group_id", containerFactory = "USerKafkaListenerContainerFactory")
	public void consumedMessageUSer(User user) {

		byte[] data = SerializationUtils.serialize(user);
		fileUpload(data, "kafkaEg");

//		try {
//			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("D:\\sample.txt"));
//			out.writeObject(user);
//		} catch (Exception e) {
//			System.out.println("" + e);
//		}
		System.out.println("Ther message is = " + user);
	}

	public String fileUpload(byte[] contents, String name) {

		try {
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(access_key, sec_key);
			InputStream stream = new ByteArrayInputStream(contents);

			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion)
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
			ObjectMetadata meta = new ObjectMetadata();
			meta.setContentLength(contents.length);
			meta.setContentType("Text");
			PutObjectRequest request = new PutObjectRequest(bucketName, "sample" + "/" + name, stream, meta);
			s3Client.putObject(request);
		} catch (Exception e) {
			System.out.println("exception" + e);
		}
		return "success";

	}
}
