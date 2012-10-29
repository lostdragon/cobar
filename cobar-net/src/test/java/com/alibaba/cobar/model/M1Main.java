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

/**
 * @author xianmao.hexm
 */
public class M1Main {

    public static void main(String[] args) {
        final M1 m1 = new M1();
        m1.start();

        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    long c = m1.getCount();
                    try {
                        Thread.sleep(2000L);
                    } catch (InterruptedException e) {
                        continue;
                    }
                    System.out.println("tps:" + (m1.getCount() - c) / 2);
                    System.out.println("  x:" + m1.getX().size());
                    System.out.println("  y:" + m1.getY().size());
                    System.out.println("==============");
                }
            }
        }.start();
    }

}
