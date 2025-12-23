package org.example;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SheetWriteLock {
    public static final Lock LOCK = new ReentrantLock();
}
