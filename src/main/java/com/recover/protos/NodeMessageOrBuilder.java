// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: demo.proto

package com.recover.protos;

public interface NodeMessageOrBuilder extends
    // @@protoc_insertion_point(interface_extends:com.recover.protos.NodeMessage)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>map&lt;string, .com.recover.protos.Node&gt; message = 1;</code>
   */
  int getMessageCount();
  /**
   * <code>map&lt;string, .com.recover.protos.Node&gt; message = 1;</code>
   */
  boolean containsMessage(
      java.lang.String key);
  /**
   * Use {@link #getMessageMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, com.recover.protos.Node>
  getMessage();
  /**
   * <code>map&lt;string, .com.recover.protos.Node&gt; message = 1;</code>
   */
  java.util.Map<java.lang.String, com.recover.protos.Node>
  getMessageMap();
  /**
   * <code>map&lt;string, .com.recover.protos.Node&gt; message = 1;</code>
   */
  /* nullable */
com.recover.protos.Node getMessageOrDefault(
      java.lang.String key,
      /* nullable */
com.recover.protos.Node defaultValue);
  /**
   * <code>map&lt;string, .com.recover.protos.Node&gt; message = 1;</code>
   */
  com.recover.protos.Node getMessageOrThrow(
      java.lang.String key);
}
