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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;

import org.blosc.JBlosc;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Lazy {@link BloscCompression} test using the abstract base class.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class BloscCompressionTest extends AbstractN5Test {

	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-test";

	/**
	 * @throws IOException
	 */
	@Override
	protected N5Writer createN5Writer() throws IOException {

		return new N5FSWriter(testDirPath);
	}

	@Override
	protected Compression[] getCompressions() {

		return Arrays.stream(new JBlosc().listCompressors().split(",")).map(
				a -> new BloscCompression(a, 6, BloscCompression.SHUFFLE, 0, 1)).toArray(BloscCompression[]::new);
	}

	@Test
	public void testDefaultNThreads() {

		final String bloscDatasetName = datasetName + "-blocsnthreadstest";
		try {
			n5.createDataset(
					bloscDatasetName,
					dimensions,
					blockSize,
					DataType.UINT64,
					new BloscCompression(
							"blosclz",
							6,
							BloscCompression.NOSHUFFLE,
							0,
							10));
		} catch (final IOException e) {
			fail(e.getMessage());
		}

		if (!n5.exists(bloscDatasetName))
			fail("Dataset does not exist");

		try {
			final DatasetAttributes info = n5.getDatasetAttributes(bloscDatasetName);
			Assert.assertArrayEquals(dimensions, info.getDimensions());
			Assert.assertArrayEquals(blockSize, info.getBlockSize());
			Assert.assertEquals(DataType.UINT64, info.getDataType());
			Assert.assertEquals(BloscCompression.class, info.getCompression().getClass());

			@SuppressWarnings("unchecked")
			final HashMap<String, Object> map = n5.getAttribute(bloscDatasetName, "compression", HashMap.class);
			Assert.assertEquals(10, ((Double)map.get("nthreads")).intValue());
			Field nThreadsField = BloscCompression.class.getDeclaredField("nthreads");
			nThreadsField.setAccessible(true);
			Assert.assertEquals(10, nThreadsField.get(info.getCompression()));

			map.remove("nthreads");
			map.put("clevel", ((Double)map.get("clevel")).intValue());
			map.put("blocksize", ((Double)map.get("blocksize")).intValue());
			map.put("shuffle", ((Double)map.get("shuffle")).intValue());
			n5.setAttribute(bloscDatasetName, "compression", map);

			final DatasetAttributes info2 = n5.getDatasetAttributes(bloscDatasetName);
			Assert.assertArrayEquals(dimensions, info2.getDimensions());
			Assert.assertArrayEquals(blockSize, info2.getBlockSize());
			Assert.assertEquals(DataType.UINT64, info2.getDataType());
			Assert.assertEquals(BloscCompression.class, info2.getCompression().getClass());
			nThreadsField = BloscCompression.class.getDeclaredField("nthreads");
			nThreadsField.setAccessible(true);
			Assert.assertEquals(1, nThreadsField.get(info2.getCompression()));
		} catch (final IOException e) {
			fail("Dataset info cannot be opened");
			e.printStackTrace();
		} catch (final IllegalAccessException | IllegalArgumentException | NoSuchFieldException e) {
			fail("Cannot access nthreads field");
			e.printStackTrace();
		}

		try {
			n5.remove(bloscDatasetName);
		} catch (final IOException e) {
			fail("Dataset info cannot be removed");
		}
	}
}
