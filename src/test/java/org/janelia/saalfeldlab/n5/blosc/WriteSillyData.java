package org.janelia.saalfeldlab.n5.blosc;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;


public class WriteSillyData {

	public static void main(String[] args) {
		
		byte[] data = new byte[]{48, 36, 24, 12, 24, 36, 48 };
		ByteArrayDataBlock blk = new ByteArrayDataBlock(new int[]{data.length}, new long[] {0}, data);
		
		BloscCompression blosc = new BloscCompression();
		File f = new File("/home/john/dev/hackathon/data/testData");
		try( FileOutputStream os = new FileOutputStream(f) ) {
			blosc.write(blk, os);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		byte[] dataDecoded = new byte[data.length];
		ByteArrayDataBlock blkDecoded = new ByteArrayDataBlock(new int[]{data.length}, new long[]{0}, dataDecoded );
		try( FileInputStream is = new FileInputStream(f) ) {
			blosc.read(blkDecoded, is);
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("read data: " + Arrays.toString(dataDecoded));
		System.out.println("done");

	}

	public static void withEncoding(String[] args) {


	}

}
