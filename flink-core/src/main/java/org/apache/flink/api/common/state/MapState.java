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
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

@PublicEvolving
public interface MapState<K, V> extends State {
    /**
     * Returns the current value for the key in the state.
     *
     * @return The operator state value corresponding to the given key.
     * @throws IOException Thrown if the system cannot access the state.
     */
    V get(K key) throws IOException;


    /**
     * Updates the operator state accessible by the key to the given key.
     *
     * @param key   The key of the state to update.
     * @param value The new value for the key.
     * @throws IOException Thrown if the system cannot access the state.
     */
    void put(K key, V value) throws IOException;

    /**
     * Deletes the operator state accessible by the key.
     *
     * @param key The key of the state to delete.
     * @throws IOException Thrown if the system cannot access the state.
     */
    void remove(K key) throws IOException;

    /**
     * Returns all the keys in the state.
     *
     * @return The list of the keys in the state.
     * @throws IOException Thrown if the system cannot access the state.
     */
    Iterable<K> keys() throws IOException;

    /**
     * Returns all the values in the state.
     *
     * @return The list of the values in the state.
     * @throws IOException Thrown if the system cannot access the state.
     */
    Iterable<V> values() throws IOException;

    /**
     * Returns an iterator to scan the state.
     *
     * @return An iterator to scan the state
     * @throws IOException Thrown if the system cannot access the state.
     */
    Iterator<Tuple2<K, V>> iterator() throws IOException;

    /**
     * Returns all the keys that are not less than the specified key (Optional).
     */
    Iterable<Tuple2<K, V>> seek(K key) throws IOException;
}