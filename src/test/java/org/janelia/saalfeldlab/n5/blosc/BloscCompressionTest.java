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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;

import org.blosc.JBlosc;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.GsonBuilder;

/**
 * Lazy {@link BloscCompression} test using the abstract base class.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class BloscCompressionTest extends AbstractN5Test {

	private static String testDirPath = createTestDirPath("n5-test");

	private static String createTestDirPath(final String dirName) {
		try {
			return Files.createTempDirectory(dirName).toString();
		} catch (final IOException exc) {
			return System.getProperty("user.home") + "/tmp/" + dirName;
		}
	}

	@Override
	protected N5Reader createN5Reader(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		return new N5FSReader(location, gson);
	}

	@Override
	protected N5Writer createN5Writer(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		return new N5FSWriter(location, gson);
	}

	@Override
	protected String tempN5Location() throws URISyntaxException {

		final String basePath = new File(tempN5PathName()).toURI().normalize().getPath();
		return new URI("file", null, basePath, null).toString();
	}

	private static String tempN5PathName() {

		try {
			final File tmpFile = Files.createTempDirectory("n5-test-").toFile();
			tmpFile.deleteOnExit();
			return tmpFile.getCanonicalPath();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override protected N5Writer createN5Writer() throws IOException, URISyntaxException {

		return new N5FSWriter(tempN5Location(), new GsonBuilder()) {
			@Override public void close() {

				super.close();
				remove();
			}
		};
	}

	@Override
	protected Compression[] getCompressions() {

		return Arrays.stream(new JBlosc().listCompressors().split(",")).map(
				a -> new BloscCompression(a, 6, BloscCompression.SHUFFLE, 0, 1)).toArray(BloscCompression[]::new);
	}

	@Test
	public void testDefaultNThreads() throws IOException, URISyntaxException {

		final String bloscDatasetName = datasetName + "-blocsnthreadstest";
		try (N5Writer n5 = createN5Writer()) {

			n5.createDataset( bloscDatasetName, dimensions, blockSize, DataType.UINT64,
					new BloscCompression(
							"blosclz",
							6,
							BloscCompression.NOSHUFFLE,
							0,
							10));

			if (!n5.exists(bloscDatasetName))
				fail("Dataset does not exist");

			try {
				final DatasetAttributes info = n5.getDatasetAttributes(bloscDatasetName);
				Assert.assertArrayEquals(dimensions, info.getDimensions());
				Assert.assertArrayEquals(blockSize, info.getBlockSize());
				Assert.assertEquals(DataType.UINT64, info.getDataType());
				Assert.assertEquals(BloscCompression.class, info.getCompression().getClass());

				@SuppressWarnings("unchecked")
				final Map<String, Object> map = n5.getAttribute(bloscDatasetName, "compression", Map.class);
				Assert.assertEquals(10, ((Double) map.get("nthreads")).intValue());
				Field nThreadsField = BloscCompression.class.getDeclaredField("nthreads");
				nThreadsField.setAccessible(true);
				Assert.assertEquals(10, nThreadsField.get(info.getCompression()));

				map.remove("nthreads");
				map.put("clevel", ((Double) map.get("clevel")).intValue());
				map.put("blocksize", ((Double) map.get("blocksize")).intValue());
				map.put("shuffle", ((Double) map.get("shuffle")).intValue());
				n5.setAttribute(bloscDatasetName, "compression", map);

				final DatasetAttributes info2 = n5.getDatasetAttributes(bloscDatasetName);
				Assert.assertArrayEquals(dimensions, info2.getDimensions());
				Assert.assertArrayEquals(blockSize, info2.getBlockSize());
				Assert.assertEquals(DataType.UINT64, info2.getDataType());
				Assert.assertEquals(BloscCompression.class, info2.getCompression().getClass());
				nThreadsField = BloscCompression.class.getDeclaredField("nthreads");
				nThreadsField.setAccessible(true);
				Assert.assertEquals(1, nThreadsField.get(info2.getCompression()));

			} catch (final IllegalAccessException | IllegalArgumentException | NoSuchFieldException e) {
				fail("Cannot access nthreads field");
				e.printStackTrace();
			}

		}
	}

}
