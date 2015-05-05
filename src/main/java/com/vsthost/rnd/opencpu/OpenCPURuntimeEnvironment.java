/*
 * Copyright (c) 2013-2014 Vehbi Sinan Tunalioglu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.vsthost.rnd.opencpu;

import com.vsthost.rnd.jpsolver.errors.ProblemError;
import com.vsthost.rnd.jpsolver.errors.RuntimeError;
import com.vsthost.rnd.jpsolver.interfaces.RuntimeEnvironment;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * Defines an OpenCPU service consumer class.
 *
 * TODO: Complete documentation.
 *
 * @author Vehbi Sinan Tunalioglu.
 */
public class OpenCPURuntimeEnvironment implements RuntimeEnvironment {

    /**
     * Defines enumeration class for allowed HTTP methods for OpenCPU communication.
     */
    public enum HTTPMethods {
        GET, POST
    }

    /**
     * Defines an HTTP status code enumeration class.
     */
    public enum HTTPStatusCode {
        // Define HTTP status codes:
        OK(200),
        CREATED(201),
        FOUND(302),
        BAD_REQUEST(400),
        BAD_GATEWAY(502),
        SERVICE_UNAVAILABLE(503);

        /**
         * Defines the numeric code of the HTTP status code.
         */
        private int code;

        /**
         * Constructor consuming the numeric HTTP status code.
         * @param code
         */
        private HTTPStatusCode(int code) {
            this.code = code;
        }

        /**
         * Returns the numeric HTTP Status code.
         * @return
         */
        public int getCode() {
            return this.code;
        }
    }

    // Define HTTP Endpoint URL schemas:
    private static final String GlobalPackagePath       = "/library/${package}";
    private static final String UserPackagePath         = "/user/${user}/library/${package}";
    private static final String CranPackagePath         = "/cran/${package}";
    private static final String BioconductorPackagePath = "/bioc/${package}";
    private static final String GithubPackagePath       = "/github/${user}/${package}";
    private static final String SessionOutputPath       = "/tmp/${key}";
    private static final String GistPath                = "/gist/${user}";
    private static final String PackageInfoPath         = "/info";
    private static final String PackageObjectPath       = "/R";
    private static final String PackageDataPath         = "/data";
    private static final String PackageManPath          = "/man";

    /**
     * Defines the base URI of the OpenCPU service.
     */
    private URI baseURI;

    /**
     * Defines the class logger.
     */
    private static Logger logger = Logger.getLogger(OpenCPURuntimeEnvironment.class.getName());

    /**
     * Default constructor for the {@link OpenCPURuntimeEnvironment}.
     *
     * @throws URISyntaxException URISyntaxException thrown.
     */
    public OpenCPURuntimeEnvironment () throws URISyntaxException {
        this.setBaseURI(new URI("http", null, "localhost", 9999, "/ocpu", null, null));
    }

    public OpenCPURuntimeEnvironment (String scheme, String host, int port, String rootPath) throws URISyntaxException {
        this.setBaseURI(new URI(scheme, null, host, port, rootPath, null, null));
    }

    public OpenCPURuntimeEnvironment (String scheme, String userInfo, String host, int port, String rootPath) throws URISyntaxException {
        this.setBaseURI(new URI(scheme, userInfo, host, port, rootPath, null, null));
    }

    public OpenCPURuntimeEnvironment (URI baseURI) throws URISyntaxException {
        this.setBaseURI(baseURI);
    }

    public OpenCPURuntimeEnvironment (String baseURI) throws URISyntaxException {
        this.setBaseURI(new URI(baseURI));
    }

    /**
     * Returns the base URI of the OpenCPU service.
     *
     * @return The base URI of the OpenCPU service.
     */
    public URI getBaseURI() {
        return baseURI;
    }

    /**
     * Sets the base URI of the OpenCPU service.
     *
     * @param baseURI The base URI of the OpenCPU service.
     */
    public void setBaseURI(URI baseURI) {
        this.baseURI = baseURI;
    }

    /**
     * Calls a remote procedure (function) which consumes and produces JSON objects.
     *
     * @param cpackage The name of the package of the function to be called.
     * @param function The name of the function to be called.
     * @param input The input as a JSON string.
     * @return The output as a JSON string.
     * @throws ProblemError Problem Error thrown.
     * @throws RuntimeError Runtime Error thrown.
     */
    public String rpc (String cpackage, String function, String input) throws ProblemError, RuntimeError {
        // Get an http client:
        HttpClient httpClient = HttpClientBuilder.create().build();

        // Construct the path.
        String URL = this.getBaseURI().toString() +
                new StrSubstitutor(new HashMap<String, String>() {{
                    put("package", cpackage);
                    put("function", function);
                }}).replace(GlobalPackagePath + PackageObjectPath + "/${function}/json?digits=6");

        // Initialize the HTTP POST request:
        HttpPost request = new HttpPost(URL);

        // Set the request content type:
        request.setHeader("Content-type", "application/json");

        // Set the payload:
        try {
            request.setEntity(new StringEntity(input));
        }
        catch (UnsupportedEncodingException e) {
            logger.severe("The encoding of the input is not supported.");
            throw new ProblemError("The encoding of the input is not supported.");
        }

        // Send the request and get the response:
        logger.info("Sending request to OpenCPU server (" + URL + ").");
        HttpResponse response = null;
        try {
            response = httpClient.execute(request);
        }
        catch (IOException e) {
            logger.severe("Cannot execute the HTTP request.");
            throw new RuntimeError("Cannot execute the HTTP request.");
        }

        // Get the response status code:
        int statusCode = response.getStatusLine().getStatusCode();

        // Check the response status code and act accordingly. Note that we don't expect a 201 code
        // as JSON I/O RPC is producing plain 200 with the result in the response body.
        if (statusCode == HTTPStatusCode.OK.getCode()) {
            logger.info("Response received successfully.");

            // Get the HTTP entity:
            HttpEntity entity = response.getEntity();

            // Check the HTTP entity:
            if (entity == null) {
                logger.severe("No content received from OpenCPU Server.");
                throw new RuntimeError("No content received from OpenCPU Server.");
            }

            // Return the results:
            try {
                return EntityUtils.toString(entity);
            }
            catch (IOException e) {
                logger.severe("Cannot read the output from OpenCPU server response.");
                throw new RuntimeError("Cannot read the output from OpenCPU server response.");
            }
        }
        else if (statusCode == HTTPStatusCode.FOUND.getCode()) {
            logger.severe("Response is redirected.");
            throw new RuntimeError("Response is redirected.");
        }
        else if (statusCode == HTTPStatusCode.BAD_REQUEST.getCode()) {
            // Declare the error message:
            String message = "";
            // Get the HTTP entity:
            HttpEntity entity = response.getEntity();

            // Check the HTTP entity:
            if (entity == null) {
                logger.severe("No content received from OpenCPU Server.");
                message = "No content received from OpenCPU Server.";
            }
            else {
                // Return the results:
                try {
                    message = EntityUtils.toString(entity);
                } catch (IOException e) {
                    logger.severe("Cannot read the output from OpenCPU server response.");
                    throw new RuntimeError("Cannot read the output from OpenCPU server response.");
                }
            }
            logger.severe("Bad request: " + message);
            throw new ProblemError("Bad Request: " + message);
        }
        else if (statusCode == HTTPStatusCode.BAD_GATEWAY.getCode()) {
            logger.severe("OpenCPU Server is not responsive (" + statusCode + ").");
            throw new RuntimeError("OpenCPU Server is not responsive (" + statusCode + ").");
        }
        else if (statusCode == HTTPStatusCode.SERVICE_UNAVAILABLE.getCode()) {
            logger.severe("OpenCPU Server is not responsive (" + statusCode + ").");
            throw new RuntimeError("OpenCPU Server is not responsive (" + statusCode + ").");
        }

        // TODO: Change the structure of this code as it reads like a VB code.
        logger.severe("Unrecognized response from the OpenCPU Server: " + statusCode);
        throw new RuntimeError("Unrecognized response from the OpenCPU Server: " + statusCode);
    }
}
