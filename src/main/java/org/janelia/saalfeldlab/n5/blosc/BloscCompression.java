/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2019 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
/**
 * Copyright (c) 2019, Stephan Saalfeld
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.blosc;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.blosc.BufferSizes;
import org.blosc.JBlosc;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * Compression using JBlosc (https://github.com/Blosc/JBlosc) compressors.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
@CompressionType("blosc")
@NameConfig.Name("blosc")
public class BloscCompression implements Compression  {

	public static final int NOSHUFFLE = 0;
	public static final int SHUFFLE = 1;
	public static final int BITSHUFFLE = 2;
	public static final int AUTOSHUFFLE = -1;

	@CompressionParameter
	@NameConfig.Parameter
	private final String cname;

	@CompressionParameter
	@NameConfig.Parameter
	private final int clevel;

	@CompressionParameter
	@NameConfig.Parameter
	private final int shuffle;

	@CompressionParameter
	@NameConfig.Parameter
	private final int blocksize;

	@CompressionParameter
	@NameConfig.Parameter
	private final int typesize;

	@CompressionParameter
	@NameConfig.Parameter
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
