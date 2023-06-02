package com.recover.bootstrap;

import com.recover.listener.Listener;

public class Main {
    public static void main(String[] args) {
        final Listener listener = new Listener();
        listener.listen();
    }
}
