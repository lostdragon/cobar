/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cobar.model;

import java.util.concurrent.BlockingQueue;

import jsr166y.LinkedTransferQueue;

/**
 * @author xianmao.hexm
 */
public class M1 {

    private long count;
    private final BlockingQueue<TransferObject> x;
    private final BlockingQueue<TransferObject> y;

    public M1() {
        this.x = new LinkedTransferQueue<TransferObject>();
        this.y = new LinkedTransferQueue<TransferObject>();
    }

    public long getCount() {
        return count;
    }

    public BlockingQueue<TransferObject> getX() {
        return x;
    }

    public BlockingQueue<TransferObject> getY() {
        return y;
    }

    public void start() {
        new Thread(new A(), "A").start();
        new Thread(new B(), "B").start();
        new Thread(new C(), "C").start();
    }

    private final class A implements Runnable {
        @Override
        public void run() {
            for (;;) {
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException e) {
                }
                for (int i = 0; i < 1000000; i++) {
                    x.offer(new TransferObject());
                }
            }
        }
    }

    private final class B implements Runnable {
        @Override
        public void run() {
            TransferObject t = null;
            for (;;) {
                try {
                    t = x.take();
                } catch (InterruptedException e) {
                    continue;
                }
                t.handle();
                y.offer(t);
            }
        }
    }

    private final class C implements Runnable {
        @Override
        public void run() {
            TransferObject t = null;
            for (;;) {
                try {
                    t = y.take();
                } catch (InterruptedException e) {
                    continue;
                }
                t.compelete();
                count++;
            }
        }
    }

}
