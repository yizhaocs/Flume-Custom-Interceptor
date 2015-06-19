package com.fuhu;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created by yizhao on 6/18/15.
 */
public class CustomInterceptor
        implements Interceptor {

    private String moreBody;

    public CustomInterceptor(String moreBody) {
        this.moreBody = moreBody;
    }

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        // This is the event's body
        String oldBody = new String(event.getBody());

        // These are the event's headers
        Map<String, String> headers = event.getHeaders();


        String newBody = moreBody + oldBody;
        event.setBody(newBody.getBytes());


        // Let the enriched event go
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> interceptedEvents =
                new ArrayList<Event>(events.size());
        for (Event event : events) {
            // Intercept any event
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }

        return interceptedEvents;
    }

    @Override
    public void close() {
        // At interceptor shutdown
    }

    public static class Builder
            implements Interceptor.Builder {

        private String moreBody;

        @Override
        public void configure(Context context) {
            // Retrieve property from flume conf
            moreBody = context.getString("moreBody");
        }

        @Override
        public Interceptor build() {
            return new CustomInterceptor(moreBody);
        }
    }
}