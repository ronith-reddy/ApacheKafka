package com.kafka.producer;

import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class WikimediaUtil {

    public static void main(String[] args) {
        System.out.println(WikimediaUtil.getWikimediaData());
    }

    public static String getWikimediaData() {

        ConnectionPool pool = new ConnectionPool(1, 5L, TimeUnit.MINUTES);
        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .connectionPool(pool)
                .connectTimeout(10000, TimeUnit.MILLISECONDS)
                .readTimeout(10000, TimeUnit.MILLISECONDS)
                .build();

        HttpUrl httpUrl = HttpUrl.parse("https://stream.wikimedia.org/v2/stream/recentchange")
                .newBuilder().build();

        Request request = new Request.Builder().get()
                .url(httpUrl)
                .build();

        try {
            Response response = okHttpClient.newCall(request).execute();
            return new String(response.body().bytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
