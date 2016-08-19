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
package org.apache.phoenix.iterate;

import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.CursorUtil;

import java.sql.SQLException;
import java.util.List;

public class CursorResultIterator extends DelegateResultIterator {
    int cacheSize;
    Tuple[] cache;
    int currentIndex = 0;
    boolean isCaching = false;
    boolean isNextNull = false;
    private String cursorName;

    public CursorResultIterator(ResultIterator delegate, String cursorName) throws SQLException {
        super(delegate);
        this.cursorName = cursorName;
        this.cacheSize = CursorUtil.getFetchSize(cursorName);
        this.cache = new Tuple[cacheSize];
    }

    private void nextIsNull(){
        isNextNull = true;
    }

    @Override
    public Tuple next() throws SQLException {
        //TODO: make sure that the underlying connections are being closed properly.
        //Namely, make sure that the underlying connections do no require being called
        //past their last record to close properly.
        if(currentIndex == cacheSize) return null;

        if(isCaching){
            //TODO: make sure the issue is not the fact that this object controls the cache, and is checking it, so the worker thread is unable to write into it (?)
            //Issue: if the while loop below has no code in it, meaning the main thread is just looping through waiting for cache[currentIndex] != null or isNextNull == true,
            //execution sometimes times out.
            //Hypothesis: when the worker thread attempts to write into cache[currentIndex], the while loop is evaluating the conditional, causing
            //some concurrency issue that drops the worker thread's write. This results in the while loop looping infinitely.
            //Solution: Make use of synchronized, wait, and notify.
            synchronized (cache){
                while(cache[currentIndex] == null && !isNextNull){
                    //Do nothing
                    try {
                        cache.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            Tuple next = cache[currentIndex];
            ++currentIndex;
            if(currentIndex == cacheSize-1 && next != null){
                CursorUtil.updateCursor(cursorName, next);
            }
            return next;
        } else{
            Tuple next = super.next();
            if(currentIndex == cacheSize && next != null){
                CursorUtil.updateCursor(cursorName, next);
            }
            new Thread(new CacheThread(cache, this)).start();
            isCaching = true;
            return next;
        }
    }

    @Override
    public void explain(List<String> planSteps) {
        super.explain(planSteps);
        planSteps.add("CLIENT CURSOR " + cursorName);
    }

    @Override
    public String toString() {
        return "CursorResultIterator [cursor=" + cursorName + "]";
    }

    private class CacheThread implements Runnable {
        Tuple[] cache;
        //TODO: get rid of parent object or cache. If you have parent object, you can access cache.
        CursorResultIterator parentObject;

        private CacheThread(Tuple[] cache, CursorResultIterator parent){
            this.cache = cache;
            this.parentObject = parent;
        }

        public void run() {
            try {
                int index = 0;
                Tuple next = parentObject.getDelegate().next();
                while(index < cache.length){
                    synchronized (cache){
                        cache[index] = next;
                        cache.notify();
                    }
                    ++index;
                    if(next == null){
                        parentObject.nextIsNull();
                        break;
                    }
                    next = parentObject.getDelegate().next();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}