package azkaban.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class SplitterOutputStream extends OutputStream {
	List<OutputStream> outputs;

	public SplitterOutputStream(OutputStream... outputs) {
		this.outputs = new ArrayList<OutputStream>(outputs.length);
		for (OutputStream output : outputs) {
			this.outputs.add(output);
		}
	}

	@Override
	public void write(int b) throws IOException {
		for (OutputStream output : outputs) {
			output.write(b);
		}
	}

	@Override
	public void write(byte[] b) throws IOException {
		for (OutputStream output : outputs) {
			output.write(b);
		}
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		for (OutputStream output : outputs) {
			output.write(b, off, len);
		}
	}

	@Override
	public void flush() throws IOException {
		for (OutputStream output : outputs) {
			output.flush();
		}
	}

	@Override
	public void close() throws IOException {
		for (OutputStream output : outputs) {
			output.close();
		}
	}

}
