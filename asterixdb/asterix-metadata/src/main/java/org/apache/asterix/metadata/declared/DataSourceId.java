/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.metadata.declared;

public class DataSourceId {

    private String dataverseName;
    private String datasourceName;
    private String fieldName;

    public DataSourceId(String dataverseName, String datasourceName) {
        this.dataverseName = dataverseName;
        this.datasourceName = datasourceName;
        this.fieldName = null;
    }

    public DataSourceId(String dataverseName, String datasourceName, String fieldName) {
        this.dataverseName = dataverseName;
        this.datasourceName = datasourceName;
        this.fieldName = fieldName;
    }

    @Override
    public String toString() {
        return dataverseName + "." + datasourceName + ", " + fieldName;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasourceName() {
        return datasourceName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void addFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
