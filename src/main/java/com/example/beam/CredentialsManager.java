package com.example.beam;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

import java.io.File;
import java.io.FileInputStream;

class CredentialsManager {

    static GoogleCredentials loadGoogleCredentials(String credentialsFile) {
        File file = new File(credentialsFile);
        try {
            FileInputStream inputStream = new FileInputStream(file);
            ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(inputStream).toBuilder().build();
            String projectId = credentials.getProjectId();
            System.out.println(projectId);
            return credentials;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}
