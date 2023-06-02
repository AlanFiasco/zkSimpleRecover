// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: demo.proto

package com.recover.protos;

/**
 * Protobuf type {@code com.recover.protos.Node}
 */
public final class Node extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:com.recover.protos.Node)
    NodeOrBuilder {
private static final long serialVersionUID = 0L;
  // Use Node.newBuilder() to construct.
  private Node(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private Node() {
    databaseName_ = "";
    path_ = "";
    context_ = "";
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new Node();
  }

  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return com.recover.protos.NodeProtos.internal_static_com_recover_protos_Node_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.recover.protos.NodeProtos.internal_static_com_recover_protos_Node_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.recover.protos.Node.class, com.recover.protos.Node.Builder.class);
  }

  private int bitField0_;
  public static final int DATABASENAME_FIELD_NUMBER = 1;
  @SuppressWarnings("serial")
  private volatile java.lang.Object databaseName_ = "";
  /**
   * <code>optional string databaseName = 1;</code>
   * @return Whether the databaseName field is set.
   */
  @java.lang.Override
  public boolean hasDatabaseName() {
    return ((bitField0_ & 0x00000001) != 0);
  }
  /**
   * <code>optional string databaseName = 1;</code>
   * @return The databaseName.
   */
  @java.lang.Override
  public java.lang.String getDatabaseName() {
    java.lang.Object ref = databaseName_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      databaseName_ = s;
      return s;
    }
  }
  /**
   * <code>optional string databaseName = 1;</code>
   * @return The bytes for databaseName.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString
      getDatabaseNameBytes() {
    java.lang.Object ref = databaseName_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      databaseName_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int PATH_FIELD_NUMBER = 2;
  @SuppressWarnings("serial")
  private volatile java.lang.Object path_ = "";
  /**
   * <code>optional string path = 2;</code>
   * @return Whether the path field is set.
   */
  @java.lang.Override
  public boolean hasPath() {
    return ((bitField0_ & 0x00000002) != 0);
  }
  /**
   * <code>optional string path = 2;</code>
   * @return The path.
   */
  @java.lang.Override
  public java.lang.String getPath() {
    java.lang.Object ref = path_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      path_ = s;
      return s;
    }
  }
  /**
   * <code>optional string path = 2;</code>
   * @return The bytes for path.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString
      getPathBytes() {
    java.lang.Object ref = path_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      path_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int CONTEXT_FIELD_NUMBER = 3;
  @SuppressWarnings("serial")
  private volatile java.lang.Object context_ = "";
  /**
   * <code>optional string context = 3;</code>
   * @return Whether the context field is set.
   */
  @java.lang.Override
  public boolean hasContext() {
    return ((bitField0_ & 0x00000004) != 0);
  }
  /**
   * <code>optional string context = 3;</code>
   * @return The context.
   */
  @java.lang.Override
  public java.lang.String getContext() {
    java.lang.Object ref = context_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      context_ = s;
      return s;
    }
  }
  /**
   * <code>optional string context = 3;</code>
   * @return The bytes for context.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString
      getContextBytes() {
    java.lang.Object ref = context_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      context_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  private byte memoizedIsInitialized = -1;
  @java.lang.Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @java.lang.Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (((bitField0_ & 0x00000001) != 0)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, databaseName_);
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, path_);
    }
    if (((bitField0_ & 0x00000004) != 0)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 3, context_);
    }
    getUnknownFields().writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) != 0)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, databaseName_);
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, path_);
    }
    if (((bitField0_ & 0x00000004) != 0)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, context_);
    }
    size += getUnknownFields().getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof com.recover.protos.Node)) {
      return super.equals(obj);
    }
    com.recover.protos.Node other = (com.recover.protos.Node) obj;

    if (hasDatabaseName() != other.hasDatabaseName()) return false;
    if (hasDatabaseName()) {
      if (!getDatabaseName()
          .equals(other.getDatabaseName())) return false;
    }
    if (hasPath() != other.hasPath()) return false;
    if (hasPath()) {
      if (!getPath()
          .equals(other.getPath())) return false;
    }
    if (hasContext() != other.hasContext()) return false;
    if (hasContext()) {
      if (!getContext()
          .equals(other.getContext())) return false;
    }
    if (!getUnknownFields().equals(other.getUnknownFields())) return false;
    return true;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasDatabaseName()) {
      hash = (37 * hash) + DATABASENAME_FIELD_NUMBER;
      hash = (53 * hash) + getDatabaseName().hashCode();
    }
    if (hasPath()) {
      hash = (37 * hash) + PATH_FIELD_NUMBER;
      hash = (53 * hash) + getPath().hashCode();
    }
    if (hasContext()) {
      hash = (37 * hash) + CONTEXT_FIELD_NUMBER;
      hash = (53 * hash) + getContext().hashCode();
    }
    hash = (29 * hash) + getUnknownFields().hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static com.recover.protos.Node parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.recover.protos.Node parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.recover.protos.Node parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.recover.protos.Node parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.recover.protos.Node parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.recover.protos.Node parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.recover.protos.Node parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.recover.protos.Node parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public static com.recover.protos.Node parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }

  public static com.recover.protos.Node parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.recover.protos.Node parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.recover.protos.Node parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @java.lang.Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(com.recover.protos.Node prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @java.lang.Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code com.recover.protos.Node}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:com.recover.protos.Node)
      com.recover.protos.NodeOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.recover.protos.NodeProtos.internal_static_com_recover_protos_Node_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.recover.protos.NodeProtos.internal_static_com_recover_protos_Node_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.recover.protos.Node.class, com.recover.protos.Node.Builder.class);
    }

    // Construct using com.recover.protos.Node.newBuilder()
    private Builder() {

    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);

    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      bitField0_ = 0;
      databaseName_ = "";
      path_ = "";
      context_ = "";
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.recover.protos.NodeProtos.internal_static_com_recover_protos_Node_descriptor;
    }

    @java.lang.Override
    public com.recover.protos.Node getDefaultInstanceForType() {
      return com.recover.protos.Node.getDefaultInstance();
    }

    @java.lang.Override
    public com.recover.protos.Node build() {
      com.recover.protos.Node result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public com.recover.protos.Node buildPartial() {
      com.recover.protos.Node result = new com.recover.protos.Node(this);
      if (bitField0_ != 0) { buildPartial0(result); }
      onBuilt();
      return result;
    }

    private void buildPartial0(com.recover.protos.Node result) {
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        result.databaseName_ = databaseName_;
        to_bitField0_ |= 0x00000001;
      }
      if (((from_bitField0_ & 0x00000002) != 0)) {
        result.path_ = path_;
        to_bitField0_ |= 0x00000002;
      }
      if (((from_bitField0_ & 0x00000004) != 0)) {
        result.context_ = context_;
        to_bitField0_ |= 0x00000004;
      }
      result.bitField0_ |= to_bitField0_;
    }

    @java.lang.Override
    public Builder clone() {
      return super.clone();
    }
    @java.lang.Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.setField(field, value);
    }
    @java.lang.Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return super.clearField(field);
    }
    @java.lang.Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return super.clearOneof(oneof);
    }
    @java.lang.Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return super.setRepeatedField(field, index, value);
    }
    @java.lang.Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.addRepeatedField(field, value);
    }
    @java.lang.Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof com.recover.protos.Node) {
        return mergeFrom((com.recover.protos.Node)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.recover.protos.Node other) {
      if (other == com.recover.protos.Node.getDefaultInstance()) return this;
      if (other.hasDatabaseName()) {
        databaseName_ = other.databaseName_;
        bitField0_ |= 0x00000001;
        onChanged();
      }
      if (other.hasPath()) {
        path_ = other.path_;
        bitField0_ |= 0x00000002;
        onChanged();
      }
      if (other.hasContext()) {
        context_ = other.context_;
        bitField0_ |= 0x00000004;
        onChanged();
      }
      this.mergeUnknownFields(other.getUnknownFields());
      onChanged();
      return this;
    }

    @java.lang.Override
    public final boolean isInitialized() {
      return true;
    }

    @java.lang.Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              databaseName_ = input.readStringRequireUtf8();
              bitField0_ |= 0x00000001;
              break;
            } // case 10
            case 18: {
              path_ = input.readStringRequireUtf8();
              bitField0_ |= 0x00000002;
              break;
            } // case 18
            case 26: {
              context_ = input.readStringRequireUtf8();
              bitField0_ |= 0x00000004;
              break;
            } // case 26
            default: {
              if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                done = true; // was an endgroup tag
              }
              break;
            } // default:
          } // switch (tag)
        } // while (!done)
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.unwrapIOException();
      } finally {
        onChanged();
      } // finally
      return this;
    }
    private int bitField0_;

    private java.lang.Object databaseName_ = "";
    /**
     * <code>optional string databaseName = 1;</code>
     * @return Whether the databaseName field is set.
     */
    public boolean hasDatabaseName() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional string databaseName = 1;</code>
     * @return The databaseName.
     */
    public java.lang.String getDatabaseName() {
      java.lang.Object ref = databaseName_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        databaseName_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string databaseName = 1;</code>
     * @return The bytes for databaseName.
     */
    public com.google.protobuf.ByteString
        getDatabaseNameBytes() {
      java.lang.Object ref = databaseName_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        databaseName_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string databaseName = 1;</code>
     * @param value The databaseName to set.
     * @return This builder for chaining.
     */
    public Builder setDatabaseName(
        java.lang.String value) {
      if (value == null) { throw new NullPointerException(); }
      databaseName_ = value;
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }
    /**
     * <code>optional string databaseName = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearDatabaseName() {
      databaseName_ = getDefaultInstance().getDatabaseName();
      bitField0_ = (bitField0_ & ~0x00000001);
      onChanged();
      return this;
    }
    /**
     * <code>optional string databaseName = 1;</code>
     * @param value The bytes for databaseName to set.
     * @return This builder for chaining.
     */
    public Builder setDatabaseNameBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      databaseName_ = value;
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }

    private java.lang.Object path_ = "";
    /**
     * <code>optional string path = 2;</code>
     * @return Whether the path field is set.
     */
    public boolean hasPath() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional string path = 2;</code>
     * @return The path.
     */
    public java.lang.String getPath() {
      java.lang.Object ref = path_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        path_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string path = 2;</code>
     * @return The bytes for path.
     */
    public com.google.protobuf.ByteString
        getPathBytes() {
      java.lang.Object ref = path_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        path_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string path = 2;</code>
     * @param value The path to set.
     * @return This builder for chaining.
     */
    public Builder setPath(
        java.lang.String value) {
      if (value == null) { throw new NullPointerException(); }
      path_ = value;
      bitField0_ |= 0x00000002;
      onChanged();
      return this;
    }
    /**
     * <code>optional string path = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearPath() {
      path_ = getDefaultInstance().getPath();
      bitField0_ = (bitField0_ & ~0x00000002);
      onChanged();
      return this;
    }
    /**
     * <code>optional string path = 2;</code>
     * @param value The bytes for path to set.
     * @return This builder for chaining.
     */
    public Builder setPathBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      path_ = value;
      bitField0_ |= 0x00000002;
      onChanged();
      return this;
    }

    private java.lang.Object context_ = "";
    /**
     * <code>optional string context = 3;</code>
     * @return Whether the context field is set.
     */
    public boolean hasContext() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <code>optional string context = 3;</code>
     * @return The context.
     */
    public java.lang.String getContext() {
      java.lang.Object ref = context_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        context_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string context = 3;</code>
     * @return The bytes for context.
     */
    public com.google.protobuf.ByteString
        getContextBytes() {
      java.lang.Object ref = context_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        context_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string context = 3;</code>
     * @param value The context to set.
     * @return This builder for chaining.
     */
    public Builder setContext(
        java.lang.String value) {
      if (value == null) { throw new NullPointerException(); }
      context_ = value;
      bitField0_ |= 0x00000004;
      onChanged();
      return this;
    }
    /**
     * <code>optional string context = 3;</code>
     * @return This builder for chaining.
     */
    public Builder clearContext() {
      context_ = getDefaultInstance().getContext();
      bitField0_ = (bitField0_ & ~0x00000004);
      onChanged();
      return this;
    }
    /**
     * <code>optional string context = 3;</code>
     * @param value The bytes for context to set.
     * @return This builder for chaining.
     */
    public Builder setContextBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      context_ = value;
      bitField0_ |= 0x00000004;
      onChanged();
      return this;
    }
    @java.lang.Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @java.lang.Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:com.recover.protos.Node)
  }

  // @@protoc_insertion_point(class_scope:com.recover.protos.Node)
  private static final com.recover.protos.Node DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.recover.protos.Node();
  }

  public static com.recover.protos.Node getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<Node>
      PARSER = new com.google.protobuf.AbstractParser<Node>() {
    @java.lang.Override
    public Node parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      Builder builder = newBuilder();
      try {
        builder.mergeFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(builder.buildPartial());
      } catch (com.google.protobuf.UninitializedMessageException e) {
        throw e.asInvalidProtocolBufferException().setUnfinishedMessage(builder.buildPartial());
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(e)
            .setUnfinishedMessage(builder.buildPartial());
      }
      return builder.buildPartial();
    }
  };

  public static com.google.protobuf.Parser<Node> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<Node> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public com.recover.protos.Node getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
