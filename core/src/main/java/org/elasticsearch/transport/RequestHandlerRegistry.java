/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import java.lang.reflect.Constructor;
import java.util.concurrent.Callable;
/**
 *
 */
public class RequestHandlerRegistry<Request extends TransportRequest> {
    ESLogger logger = Loggers.getLogger(RequestHandlerRegistry.class);
    private final String action;
    private final TransportRequestHandler<Request> handler;
    private final boolean forceExecution;
    private final String executor;
    private final Callable<Request> requestFactory;

    RequestHandlerRegistry(String action, Class<Request> request, TransportRequestHandler<Request> handler,
                           String executor, boolean forceExecution) {
        this(action, new ReflectionFactory<>(request), handler, executor, forceExecution);
    }

    public RequestHandlerRegistry(String action, Callable<Request> requestFactory, TransportRequestHandler<Request> handler, String executor, boolean forceExecution) {
        this.action = action;
        this.requestFactory = requestFactory;
        assert newRequest() != null;
        this.handler = handler;
        this.forceExecution = forceExecution;
        this.executor = executor;
        logger.info("===RequestHandlerRegistry===49==="+action+"==="+executor+"==="+handler.getClass().getName()+"==="+requestFactory.getClass().getName());//try { Integer.parseInt("RequestHandlerRegistry"); }catch (Exception e){logger.error("===", e);}
    }
    public String getAction() {
        return action;
    }

    public Request newRequest() {
        try {
            return requestFactory.call();
        } catch (Exception e) {
            throw new IllegalStateException("failed to instantiate request ", e);
        }
    }

    public TransportRequestHandler<Request> getHandler() {
        return handler;
    }

    public boolean isForceExecution() {
        return forceExecution;
    }

    public String getExecutor() {
        return executor;
    }

    private final static class ReflectionFactory<Request> implements Callable<Request> {
        private final Constructor<Request> requestConstructor;

        public ReflectionFactory(Class<Request> request) {
            try {
                this.requestConstructor = request.getDeclaredConstructor();
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("failed to create constructor (does it have a default constructor?) for request " + request, e);
            }
            this.requestConstructor.setAccessible(true);
        }

        @Override
        public Request call() throws Exception {
            return requestConstructor.newInstance();
        }
    }
}
