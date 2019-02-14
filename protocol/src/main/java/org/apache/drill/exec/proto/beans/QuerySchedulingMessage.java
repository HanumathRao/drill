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

public final class QuerySchedulingMessage implements Externalizable, Message<QuerySchedulingMessage>, Schema<QuerySchedulingMessage>
{

    public static Schema<QuerySchedulingMessage> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static QuerySchedulingMessage getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final QuerySchedulingMessage DEFAULT_INSTANCE = new QuerySchedulingMessage();

    
    private SchedulingMessageType status;
    private Integer queueID;
    private QueryId queryId;
    private DrillbitEndpoint sender;

    public QuerySchedulingMessage()
    {
        
    }

    public QuerySchedulingMessage(
        SchedulingMessageType status,
        Integer queueID,
        QueryId queryId,
        DrillbitEndpoint sender
    )
    {
        this.status = status;
        this.queueID = queueID;
        this.queryId = queryId;
        this.sender = sender;
    }

    // getters and setters

    // status

    public SchedulingMessageType getStatus()
    {
        return status;
    }

    public QuerySchedulingMessage setStatus(SchedulingMessageType status)
    {
        this.status = status;
        return this;
    }

    // queueID

    public Integer getQueueID()
    {
        return queueID;
    }

    public QuerySchedulingMessage setQueueID(Integer queueID)
    {
        this.queueID = queueID;
        return this;
    }

    // queryId

    public QueryId getQueryId()
    {
        return queryId;
    }

    public QuerySchedulingMessage setQueryId(QueryId queryId)
    {
        this.queryId = queryId;
        return this;
    }

    // sender

    public DrillbitEndpoint getSender()
    {
        return sender;
    }

    public QuerySchedulingMessage setSender(DrillbitEndpoint sender)
    {
        this.sender = sender;
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

    public Schema<QuerySchedulingMessage> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public QuerySchedulingMessage newMessage()
    {
        return new QuerySchedulingMessage();
    }

    public Class<QuerySchedulingMessage> typeClass()
    {
        return QuerySchedulingMessage.class;
    }

    public String messageName()
    {
        return QuerySchedulingMessage.class.getSimpleName();
    }

    public String messageFullName()
    {
        return QuerySchedulingMessage.class.getName();
    }

    public boolean isInitialized(QuerySchedulingMessage message)
    {
        return 
            message.status != null 
            && message.queueID != null 
            && message.queryId != null 
            && message.sender != null;
    }

    public void mergeFrom(Input input, QuerySchedulingMessage message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.status = SchedulingMessageType.valueOf(input.readEnum());
                    break;
                case 2:
                    message.queueID = input.readInt32();
                    break;
                case 3:
                    message.queryId = input.mergeObject(message.queryId, QueryId.getSchema());
                    break;

                case 4:
                    message.sender = input.mergeObject(message.sender, DrillbitEndpoint.getSchema());
                    break;

                default:
                    input.handleUnknownField(number, this);
            }   
        }
    }


    public void writeTo(Output output, QuerySchedulingMessage message) throws IOException
    {
        if(message.status == null)
            throw new UninitializedMessageException(message);
        output.writeEnum(1, message.status.number, false);

        if(message.queueID == null)
            throw new UninitializedMessageException(message);
        output.writeInt32(2, message.queueID, false);

        if(message.queryId == null)
            throw new UninitializedMessageException(message);
        output.writeObject(3, message.queryId, QueryId.getSchema(), false);


        if(message.sender == null)
            throw new UninitializedMessageException(message);
        output.writeObject(4, message.sender, DrillbitEndpoint.getSchema(), false);

    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "status";
            case 2: return "queueID";
            case 3: return "queryId";
            case 4: return "sender";
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
        __fieldMap.put("status", 1);
        __fieldMap.put("queueID", 2);
        __fieldMap.put("queryId", 3);
        __fieldMap.put("sender", 4);
    }
    
}
