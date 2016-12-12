/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;

/**
 * A {@link StateDescriptor} for MapState.
 *
 * @param <K> The type of the keys in the map state.
 * @param <V> The type of the values in the map state.
 */
@PublicEvolving
public class MapStateDescriptor<K, V> extends StateDescriptor<MapState<K, V>, V> {
    private static final long serialVersionUID = 1L;

    private TypeSerializer<K> keySerializer;

    /**
     * The type information describing the value type. Only used to lazily create the serializer
     * and dropped during serialization
     */
    private transient TypeInformation<K> keyTypeInfo;

    /**
     * Creates a new MapStateDescriptor with the given name and map element type.
     *
     * @param name           The (unique) name for the state.
     * @param keyTypeClass   The type of the keys in the state.
     * @param valueTypeClass The type of the values in the state.
     */
    public MapStateDescriptor(
            String name,
            Class<K> keyTypeClass,
            Class<V> valueTypeClass,
            V defaultValue
    ) {
        super(name, valueTypeClass, defaultValue);

        this.keyTypeInfo = null;
        if (keyTypeClass != null) {
            try {
                this.keyTypeInfo = TypeExtractor.createTypeInfo(keyTypeClass);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Cannot create full type information based on the given class.", e);
            }
        }
    }

    /**
     * Creates a new MapStateDescriptor with the given name and map element type.
     *
     * @param name          The (unique) name for the state.
     * @param keyTypeInfo   The type of the keys in the state
     * @param valueTypeInfo The type of the values in the state.
     */
    public MapStateDescriptor(
            String name,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<V> valueTypeInfo,
            V defaultValue
    ) {
        super(name, valueTypeInfo, defaultValue);
        this.keyTypeInfo = keyTypeInfo;
    }

    /**
     * Creates a new MapStateDescriptor with the given name and map element type.
     *
     * @param name                The (unique) name for the state.
     * @param keyTypeSerializer   The type serializer for the map keys.
     * @param valueTypeSerializer The type serializer for the map values.
     */
    public MapStateDescriptor(
            String name,
            TypeSerializer<K> keyTypeSerializer,
            TypeSerializer<V> valueTypeSerializer,
            V defaultValue
    ) {
        super(name, valueTypeSerializer, defaultValue);
        this.keySerializer = keyTypeSerializer;
    }

    // ------------------------------------------------------------------------

    @Override
    public MapState<K, V> bind(StateBackend stateBackend) throws Exception {
        return stateBackend.createMapState(this);
    }

    @Override
    public boolean isSerializerInitialized() {
        if (super.isSerializerInitialized()) {
            return keySerializer != null;
        }

        return false;
    }

    @Override
    public void initializeSerializerUnlessSet(ExecutionConfig executionConfig) {
        super.initializeSerializerUnlessSet(executionConfig);

        if (keySerializer == null) {
            if (keyTypeInfo != null) {
                keySerializer = keyTypeInfo.createSerializer(executionConfig);
            } else {
                throw new IllegalStateException(
                        "Cannot initialize serializer after TypeInformation was dropped during serialization");
            }
        }
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public TypeSerializer<V> getValueSerializer() {
        return getSerializer();
    }

    @Override
    public int hashCode() {
        return name.hashCode() + 41;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        } else {
            StateDescriptor<?, ?> that = (StateDescriptor<?, ?>) o;
            return this.name.equals(that.name);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() +
               "{name=" + name +
               ", defaultValue=" + defaultValue +
               ", serializer=" + serializer +
               '}';
    }
}
