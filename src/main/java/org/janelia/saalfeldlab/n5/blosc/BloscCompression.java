package org.janelia.saalfeldlab.n5.blosc;

import java.nio.ByteBuffer;
import org.blosc.BufferSizes;
import org.blosc.JBlosc;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * Compression using JBlosc (https://github.com/Blosc/JBlosc) compressors.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
@CompressionType("blosc")
public class BloscCompression implements Compression  {

	// Final variables corresponding to org.blosc.Shuffle
	public static final int NOSHUFFLE = 0;
	public static final int SHUFFLE = 1;
	public static final int BITSHUFFLE = 2;

	@CompressionParameter
	private final String cname;

	@CompressionParameter
	private final int clevel;

	@CompressionParameter
	private final int shuffle;

	@CompressionParameter
	private final int blocksize;

	@CompressionParameter
	private final int typesize;

	@CompressionParameter
	private int nthreads;

	private static final transient JBlosc blosc = new JBlosc();

	public String getCname() {

		return cname;
	}

	public int getClevel() {

		return clevel;
	}

	public int getShuffle() {

		return shuffle;
	}

	public int getBlocksize() {

		return blocksize;
	}

	public int getTypesize() {

		return typesize;
	}

	public int getNthreads() {

		return nthreads;
	}

	public void setNthreads(final int nthreads) {

		this.nthreads = nthreads;
	}

	public BloscCompression() {

		this.cname = "blosclz";
		this.clevel = 6;
		this.shuffle = NOSHUFFLE;
		this.blocksize = 0; // auto
		this.typesize = 1;
		this.nthreads = 1;
	}

	public BloscCompression(
			final String cname,
			final int clevel,
			final int shuffle,
			final int blocksize,
			final int typesize,
			final int nthreads) {

		this.cname = cname;
		this.clevel = clevel;
		this.shuffle = shuffle;
		this.blocksize = blocksize;
		this.typesize = typesize;
		this.nthreads = nthreads;
	}

	public BloscCompression(
			final String cname,
			final int clevel,
			final int shuffle,
			final int blocksize,
			final int nthreads) {

		this(cname, clevel, shuffle, blocksize, 1, nthreads);
	}

	public BloscCompression(final BloscCompression template) {

		this.cname = template.cname;
		this.clevel = template.clevel;
		this.shuffle = template.shuffle;
		this.blocksize = template.blocksize;
		this.typesize = template.typesize;
		this.nthreads = template.nthreads;
	}

	private byte[] decode(final byte[] data, final byte[] dstBuffer) {
		final ByteBuffer src = ByteBuffer.wrap(data);
		final ByteBuffer dst;
		if (dstBuffer != null) {
			dst = ByteBuffer.wrap(dstBuffer);
		} else {
			final BufferSizes sizes = blosc.cbufferSizes(src);
			final int dstSize = (int)sizes.getNbytes();
			dst = ByteBuffer.allocate(dstSize);
		}
		JBlosc.decompressCtx(src, dst, dst.capacity(), nthreads);
		return dst.array();
	}

	@Override
	public ReadData decode(final ReadData readData) {
		return ReadData.from(decode(readData.allBytes(), null));
	}

	@Override
	public ReadData encode(final ReadData readData) {
		final byte[] serialized = readData.allBytes();
		final ByteBuffer src = ByteBuffer.wrap(serialized);
		final ByteBuffer dst = ByteBuffer.allocate(serialized.length + JBlosc.OVERHEAD);
		JBlosc.compressCtx(clevel, shuffle, 1, src, src.limit(), dst, dst.limit(), cname, blocksize, nthreads);
		final BufferSizes sizes = blosc.cbufferSizes(dst);
		final int dstSize = (int)sizes.getCbytes();
		return ReadData.from(dst.array(), 0, dstSize);
	}
}
