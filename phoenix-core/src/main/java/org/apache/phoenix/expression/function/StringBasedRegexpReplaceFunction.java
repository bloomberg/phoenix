/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression.function;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.util.regex.AbstractBasePattern;
import org.apache.phoenix.expression.util.regex.JavaPattern;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;

public class StringBasedRegexpReplaceFunction extends RegexpReplaceFunction {

    public StringBasedRegexpReplaceFunction() {
    }

    public StringBasedRegexpReplaceFunction(List<Expression> children) {
        super(children);
    }

    public static Expression create(List<Expression> children, StatementContext context) {
        QueryServices services = context.getConnection().getQueryServices();
        boolean useByteBasedRegex =
                services.getProps().getBoolean(QueryServices.USE_BYTE_BASED_REGEX_ATTRIB,
                        QueryServicesOptions.DEFAULT_USE_BYTE_BASED_REGEX);
        if (useByteBasedRegex) {
            return new ByteBasedRegexpReplaceFunction(children);
        } else {
            return new StringBasedRegexpReplaceFunction(children);
        }
    }

    @Override
    protected AbstractBasePattern compilePatternSpec(String value) {
        return new JavaPattern(value, Pattern.DOTALL);
    }

}
