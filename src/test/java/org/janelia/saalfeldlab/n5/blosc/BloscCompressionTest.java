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
package org.janelia.saalfeldlab.n5.blosc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import org.blosc.JBlosc;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

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
	public void testDefaultNThreads() throws IOException, URISyntaxException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

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

				final DatasetAttributes info = n5.getDatasetAttributes(bloscDatasetName);

				BytesCodec[] codecs = info.getCodecs();
				assertEquals( 1, codecs.length);
				BytesCodec compression = codecs[0];
				assertEquals(BloscCompression.class, compression.getClass());

				final JsonObject obj = n5.getAttribute(bloscDatasetName, "compression", JsonElement.class).getAsJsonObject();
				Assert.assertEquals(10, obj.getAsJsonObject().get("nthreads").getAsInt());
				Field nThreadsField = BloscCompression.class.getDeclaredField("nthreads");
				nThreadsField.setAccessible(true);
				Assert.assertEquals(10, nThreadsField.get(compression));

				obj.remove("nthreads");
				n5.setAttribute(bloscDatasetName, "compression", obj);

				final DatasetAttributes info2 = n5.getDatasetAttributes(bloscDatasetName);
				BytesCodec[] codecs2 = info2.getCodecs();
				assertEquals(1, codecs2.length);
				BytesCodec compression2 = codecs2[0];

				Assert.assertEquals(BloscCompression.class, compression2.getClass());
				nThreadsField = BloscCompression.class.getDeclaredField("nthreads");
				nThreadsField.setAccessible(true);

				// TODO re-enable when we decide whether the compression field will be deprecated
//				Assert.assertEquals(1, nThreadsField.get(info2.getCompression()));
		}
	}

	@Ignore("This unit test is ignored, since it can only be run on a machine with libblosc installed.")
	@Test
	public void testBloscCompressionEncodeDecode() throws IOException
	{
		Random random = new Random();

		int n = 16;
		byte[] inputData = new byte[n];
		IntStream.range(0, n).forEach( i -> {
			inputData[i] = (byte)(random.nextInt());
		});
		System.out.println("input data:" + Arrays.toString(inputData));

       // encode
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		BytesCodec codec = new BloscCompression();

		byte[] encodedData = codec.encode(ReadData.from(inputData)).allBytes();
		System.out.println( "encoded data: " + Arrays.toString(encodedData));

        // decode
		ByteArrayInputStream is = new ByteArrayInputStream(encodedData);
		ReadData decodedIs = codec.decode(ReadData.from(is));
		byte[] decodedData = new byte[n];

		System.out.println("decoded data:" + Arrays.toString(decodedData));

		assertArrayEquals( inputData, decodedData );
	}

}
