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


import com.vsthost.rnd.jpsolver.data.Value;
import com.vsthost.rnd.jpsolver.data.ValueBuilder;
import com.vsthost.rnd.jpsolver.errors.ProblemError;
import com.vsthost.rnd.jpsolver.errors.ProgramError;
import com.vsthost.rnd.jpsolver.errors.ProgramWarning;
import com.vsthost.rnd.jpsolver.errors.RuntimeError;
import com.vsthost.rnd.jpsolver.interfaces.Problem;
import com.vsthost.rnd.jpsolver.interfaces.Program;
import com.vsthost.rnd.jpsolver.interfaces.Solution;

import java.util.*;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * Defines an RPC service consumer for OpenRPC server.
 *
 * TODO: Complete documentation.
 *
 * @author Vehbi Sinan Tunalioglu
 */
public class RPCProgram implements Program {

    /**
     * Defines the R package of the function to be invoked.
     */
    private String cpackage;

    /**
     * Defines the function to invoke.
     */
    private String function;

    /**
     * Defines the OpenCPU Server as a runtime for the computation.
     */
    private OpenCPURuntimeEnvironment runtime;

    /**
     * Defines the logger.
     */
    private final Logger logger = Logger.getLogger(this.getClass().getPackage().getName());

    /**
     * Constructor which consumes runtime, R package and function specifications
     * for the computation.
     *
     * @param runtime The OpenCPU Server as a runtime for the computation.
     * @param cpackage The R pacakge of the function to be invoked.
     * @param function The function to be invoked.
     */
    public RPCProgram (OpenCPURuntimeEnvironment runtime, String cpackage, String function) {
        this.setRuntime(runtime);
        this.setCpackage(cpackage);
        this.setFunction(function);
    }

    public String getCpackage() {
        return cpackage;
    }

    public void setCpackage(String cpackage) {
        this.cpackage = cpackage;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public OpenCPURuntimeEnvironment getRuntime() {
        return runtime;
    }

    public void setRuntime(OpenCPURuntimeEnvironment runtime) {
        this.runtime = runtime;
    }

    @Override
    public Solution compute(Problem problem) throws ProgramError {
        try {
            // Compute and retrieve the data:
            String data = this.getRuntime().rpc(this.getCpackage(), this.getFunction(), problem.toJson());
            logger.info(data);

            // Cast data:
            Value computedData = ValueBuilder.fromJson(data);

            // Return a Solution instance:
            return new Solution() {
                // Save the computed value:
                private Value value = computedData;

                @Override
                public boolean hasValue() {
                    return this.value != null;
                }

                @Override
                public Value getValue() {
                    return this.value;
                }

                @Override
                public boolean hasWarnings() {
                    return false;
                }

                @Override
                public List<ProgramWarning> getWarnings() {
                    return new ArrayList<>();
                }

                @Override
                public boolean hasAttribute(String name) {
                    return false;
                }

                @Override
                public Object getAttribute(String name) {
                    return null;
                }

                @Override
                public Map<String, Object> getAttributes() {
                    return new HashMap<>();
                }

                @Override
                public String toJson() {
                    return this.value.toJson();
                }
            };
        }
        catch (RuntimeError runtimeError) {
            throw new ProgramError(runtimeError.getMessage());
        }
        catch (ProblemError problemError) {
            throw new ProgramError(problemError.getMessage());
        }
    }

    @Override
    public Future<Solution> computeAsync(Problem problem) {
        return null;
    }
}
