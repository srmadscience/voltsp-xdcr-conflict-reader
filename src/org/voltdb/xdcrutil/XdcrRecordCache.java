package org.voltdb.xdcrutil;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import java.util.HashMap;

import org.voltdb.xdcrutil.*;

public class XdcrRecordCache {

    private static XdcrRecordCache instance = null;

    HashMap<String, XdcrConflictMessage> theCache = new HashMap<String, XdcrConflictMessage>();

    final int DEFAULT_SIZE = 100;

    long lastStatsTime = System.currentTimeMillis();

    protected XdcrRecordCache() {
        // Exists only to defeat instantiation.
    }

    public static XdcrRecordCache getInstance() {
        if (instance == null) {
            instance = new XdcrRecordCache();
        }
        return instance;
    }

    public void reset() {
        synchronized (theCache) {
            theCache = new HashMap<String, XdcrConflictMessage>();

        }
    }

    public XdcrConflictMessage get(String type) {
        XdcrConflictMessage h = null;

        synchronized (theCache) {
            h = theCache.get(type);

        }

        return h;
    }

    public void put(XdcrConflictMessage message) {

        synchronized (theCache) {
            theCache.put(message.getConflictPK(), message);

        }

    }

    @Override
    public String toString() {
        String data = "";
        synchronized (theCache) {

            data = theCache.toString();
        }

        return data;
    }

    public String toStringIfOlderThanMs(int statsInterval) {

        String data = "";

        if (lastStatsTime + statsInterval < System.currentTimeMillis()) {
            synchronized (theCache) {

                data = theCache.toString();
                lastStatsTime = System.currentTimeMillis();
            }

        }
        return data;
    }

    public void remove(String string) {

        synchronized (theCache) {
            theCache.remove(string);

        }

    }

}