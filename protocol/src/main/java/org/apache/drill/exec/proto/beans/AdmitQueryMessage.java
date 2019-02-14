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
// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from protobuf

package org.apache.drill.exec.proto.beans;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.UninitializedMessageException;

public final class AdmitQueryMessage implements Externalizable, Message<AdmitQueryMessage>, Schema<AdmitQueryMessage>
{

    public static Schema<AdmitQueryMessage> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static AdmitQueryMessage getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final AdmitQueryMessage DEFAULT_INSTANCE = new AdmitQueryMessage();

    
    private Integer queueID;
    private QueryId queryId;

    public AdmitQueryMessage()
    {
        
    }

    public AdmitQueryMessage(
        Integer queueID,
        QueryId queryId
    )
    {
        this.queueID = queueID;
        this.queryId = queryId;
    }

    // getters and setters

    // queueID

    public Integer getQueueID()
    {
        return queueID;
    }

    public AdmitQueryMessage setQueueID(Integer queueID)
    {
        this.queueID = queueID;
        return this;
    }

    // queryId

    public QueryId getQueryId()
    {
        return queryId;
    }

    public AdmitQueryMessage setQueryId(QueryId queryId)
    {
        this.queryId = queryId;
        return this;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException
    {
        GraphIOUtil.mergeDelimitedFrom(in, this, this);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        GraphIOUtil.writeDelimitedTo(out, this, this);
    }

    // message method

    public Schema<AdmitQueryMessage> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public AdmitQueryMessage newMessage()
    {
        return new AdmitQueryMessage();
    }

    public Class<AdmitQueryMessage> typeClass()
    {
        return AdmitQueryMessage.class;
    }

    public String messageName()
    {
        return AdmitQueryMessage.class.getSimpleName();
    }

    public String messageFullName()
    {
        return AdmitQueryMessage.class.getName();
    }

    public boolean isInitialized(AdmitQueryMessage message)
    {
        return 
            message.queueID != null 
            && message.queryId != null;
    }

    public void mergeFrom(Input input, AdmitQueryMessage message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.queueID = input.readInt32();
                    break;
                case 2:
                    message.queryId = input.mergeObject(message.queryId, QueryId.getSchema());
                    break;

                default:
                    input.handleUnknownField(number, this);
            }   
        }
    }


    public void writeTo(Output output, AdmitQueryMessage message) throws IOException
    {
        if(message.queueID == null)
            throw new UninitializedMessageException(message);
        output.writeInt32(1, message.queueID, false);

        if(message.queryId == null)
            throw new UninitializedMessageException(message);
        output.writeObject(2, message.queryId, QueryId.getSchema(), false);

    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "queueID";
            case 2: return "queryId";
            default: return null;
        }
    }

    public int getFieldNumber(String name)
    {
        final Integer number = __fieldMap.get(name);
        return number == null ? 0 : number.intValue();
    }

    private static final java.util.HashMap<String,Integer> __fieldMap = new java.util.HashMap<String,Integer>();
    static
    {
        __fieldMap.put("queueID", 1);
        __fieldMap.put("queryId", 2);
    }
    
}
