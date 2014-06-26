using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
	static class IntensityBinConverterInt32
	{
		/// <summary>
		/// Convert a list of intensity information by bin to a zero length encoded byte array
		/// </summary>
		/// <param name="binToIntensityMap"></param>
		/// <param name="timeOffset"></param>
		/// <param name="spectra"></param>
		/// <param name="tic"></param>
		/// <param name="bpi"></param>
		/// <param name="binNumberMaxIntensity"></param>
		/// <returns></returns>
		public static int Encode(
			List<KeyValuePair<int, int>> binToIntensityMap,
			int timeOffset,
			out byte[] spectra,
			out double tic,
			out double bpi,
			out int binNumberMaxIntensity)
		{
			spectra = null;
			tic = 0;
			bpi = 0;
			binNumberMaxIntensity = 0;

			int arraySize = binToIntensityMap.Count;

			// RLZE - convert 0s to negative multiples as well as calculate TIC and BPI, BPI_MZ
			var rlzeDataList = new List<int>();

			// 32-bit integers are 4 bytes
			const int dataTypeSize = 4;

			// Calculate TIC and BPI while run length zero encoding
			int previousBin = int.MinValue;

			rlzeDataList.Add(-(timeOffset + binToIntensityMap[0].Key));
			for (int i = 0; i < arraySize; i++)
			{
				int intensity = binToIntensityMap[0].Value;
				int currentBin = binToIntensityMap[0].Key;

				// the intensities will always be positive integers
				tic += intensity;
				if (bpi < intensity)
				{
					bpi = intensity;
					binNumberMaxIntensity = currentBin;
				}

				if (i != 0 && currentBin != previousBin + 1)
				{
					// since the bin numbers are not continuous, add a negative index to the array
					// and in some cases we have to add the offset from the previous index
					rlzeDataList.Add(previousBin - currentBin + 1);
				}

				rlzeDataList.Add(intensity);

				previousBin = currentBin;
			}

			// Compress intensities
			int nonZeroCount = 0;

			var nrlze = rlzeDataList.Count;
			int[] runLengthZeroEncodedData = rlzeDataList.ToArray();

			var compressedData = new byte[nrlze * dataTypeSize * 5];
			if (nrlze > 0)
			{
				var byteBuffer = new byte[nrlze * dataTypeSize];
				Buffer.BlockCopy(runLengthZeroEncodedData, 0, byteBuffer, 0, nrlze * dataTypeSize);
				nonZeroCount = LZFCompressionUtil.Compress(
					ref byteBuffer,
					nrlze * dataTypeSize,
					ref compressedData,
					compressedData.Length);
			}

			if (nonZeroCount != 0)
			{
				spectra = new byte[nonZeroCount];
				Array.Copy(compressedData, spectra, nonZeroCount);
			}

			return nonZeroCount;

		}

	}
}


