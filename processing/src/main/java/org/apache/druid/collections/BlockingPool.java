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

package org.apache.druid.collections;

import java.util.ArrayList;
import java.util.List;

public interface BlockingPool<T>
{
  int maxSize();


  /**
   * Take resources from the pool, waiting up to the
   * specified wait time if necessary for elements of the given number to become available.
   *
   * @param elementNum number of resources to take
   * @param timeoutMs  maximum time to wait for resources, in milliseconds.
   * @return a list of resource holders. An empty list is returned if {@code elementNum} resources aren't available.
   */
  List<ResourceHolder<T>> takeBatch(int elementNum, long timeoutMs);

  /**
   * Take resources from the pool, waiting if necessary until the elements of the given number become available.
   *
   * @param elementNum number of resources to take
   * @return a list of resource holders. An empty list is returned if {@code elementNum} resources aren't available.
   */
  List<ResourceHolder<T>> takeBatch(int elementNum);

  /**
   * Returns the count of the requests waiting to acquire a batch of resources.
   *
   * @return count of pending requests
   */
  long getPendingRequests();

  default BlockingPool<T> newSubPool() {
    return new BlockingPool<T>()
    {

      private List<ResourceHolder<T>> holders = new ArrayList<ResourceHolder<T>>();
      private boolean locked;
      private List<ResourceHolder<T>> realHolders;


      public void lockSubPool() {
        if (locked) {
          return;
        }
        realHolders = BlockingPool.this.takeBatch(holders.size());
        locked = true;
      }

      @Override
      public int maxSize()
      {
        return BlockingPool.this.maxSize();
      }

      @Override
      // FIXME: this timeout will not work anymore...
      public List<ResourceHolder<T>> takeBatch(int elementNum, long timeoutMs)
      {
        return takeBatch(elementNum);
      }

      @Override
      public List<ResourceHolder<T>> takeBatch(int elementNum)
      {
        if(locked) {
          throw new RuntimeException("invalid usage pattern!");
        }
        return createLazyHolders(elementNum);
      }

      private List<ResourceHolder<T>> createLazyHolders(int elementNum)
      {

        List<ResourceHolder<T>> ret=new ArrayList<>();
        for (int i=0;i<elementNum;i++) {
          LazyHolder<T> lh = new LazyHolder<>(holders.size());
          holders.add(lh);
          ret.add(lh);
        }
        return ret;
      }

      class LazyHolder<T> implements ResourceHolder<T>{

        private int idx;

        LazyHolder(int idx)
        {
          this.idx = idx;
        }

        @Override
        public T get()
        {
          lockSubPool();
          ResourceHolder<?> realHolder = realHolders.get(idx);
          return          (T) realHolder.get();
        }

        @Override
        public void close()
        {
          // accept NPE on second call?!
          if (locked) {
            realHolders.get(idx).close();
          }
        }
        @Override
        public ResourceHolder<T> newReference()
        {
          return (ResourceHolder<T>) realHolders.get(idx).newReference();
        }
      }

      @Override
      public long getPendingRequests()
      {
        if(locked) {
          return 0;
        } else {
          return holders.size();
        }
      }
    };

  }
}
