package utils;

import java.io.ByteArrayOutputStream;

public class CustomByteArrayOutputStream extends ByteArrayOutputStream {
	public CustomByteArrayOutputStream() {
	}

	public CustomByteArrayOutputStream(int size) {
		super(size);
	}

	public int getCount() {
		return super.count;
	}

	public byte[] getByteBuffer() {
		return super.buf;
	}
}
