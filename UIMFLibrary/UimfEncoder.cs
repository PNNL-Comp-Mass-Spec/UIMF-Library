﻿using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
    internal static class UimfEncoder
    {
        /// <summary>
        /// Convert an array of intensities to a zero length encoded byte array
        /// </summary>
        /// <param name="intensities"></param>
        /// <param name="spectra"></param>
        /// <param name="tic"></param>
        /// <param name="bpi"></param>
        /// <param name="indexOfMaxIntensity"></param>
        /// <returns>
        /// Number of non-zero data points
        /// </returns>
        public static int Encode(this short[] intensities, out byte[] spectra,
            out double tic,
            out double bpi,
            out int indexOfMaxIntensity)
        {
            spectra = null;
            tic = 0;
            bpi = 0;
            indexOfMaxIntensity = 0;

            // RLZE - convert 0s to negative multiples as well as calculate TIC and BPI, BPI_MZ
            short zeroCount = 0;
            var rlzeDataList = new List<short>();
            var nonZeroCount = 0;

            // 16-bit integers are 2 bytes
            const int dataTypeSize = 2;

            // Calculate TIC and BPI while run length zero encoding
            for (var i = 0; i < intensities.Length; i++)
            {
                var intensity = intensities[i];
                if (intensity > 0)
                {
                    // TIC is just the sum of all intensities
                    tic += intensity;
                    if (intensity > bpi)
                    {
                        bpi = intensity;
                        indexOfMaxIntensity = i;
                    }

                    if (zeroCount < 0)
                    {
                        rlzeDataList.Add(zeroCount);
                        zeroCount = 0;
                    }

                    rlzeDataList.Add(intensity);
                    nonZeroCount++;
                }
                else
                {
                    if (zeroCount == Int16.MinValue)
                    {
                        // Too many zeroes; need to append two points to rlzeDataList to avoid an overflow
                        rlzeDataList.Add(zeroCount);
                        rlzeDataList.Add((short)0);
                        zeroCount = 0;
                    }

                    zeroCount--;
                }
            }

            // Compress intensities
            var nlzf = 0;
            var nrlze = rlzeDataList.Count;
            var runLengthZeroEncodedData = rlzeDataList.ToArray();

            var compressedData = new byte[nrlze * dataTypeSize * 5];

            if (nrlze > 0)
            {
                var byteBuffer = new byte[nrlze * dataTypeSize];
                Buffer.BlockCopy(runLengthZeroEncodedData, 0, byteBuffer, 0, nrlze * dataTypeSize);
                nlzf = LZFCompressionUtil.Compress(
                    ref byteBuffer,
                    nrlze * dataTypeSize,
                    ref compressedData,
                    compressedData.Length);
            }

            if (nlzf != 0)
            {
                spectra = new byte[nlzf];
                Array.Copy(compressedData, spectra, nlzf);
            }

            return nonZeroCount;

        }

        /// <summary>
        /// Convert an array of intensities to a zero length encoded byte array
        /// </summary>
        /// <param name="intensities"></param>
        /// <param name="spectra"></param>
        /// <param name="tic"></param>
        /// <param name="bpi"></param>
        /// <param name="indexOfMaxIntensity"></param>
        /// <returns>
        /// Number of non-zero data points
        /// </returns>
        public static int Encode(
            this IEnumerable<int> intensities,
            out byte[] spectra,
            out double tic,
            out double bpi,
            out int indexOfMaxIntensity)
        {
            spectra = null;
            tic = 0;
            bpi = 0;
            indexOfMaxIntensity = 0;

            // RLZE - convert 0s to negative multiples as well as calculate TIC and BPI, BPI_MZ
            short zeroCount = 0;
            var rlzeDataList = new List<int>();
            var nonZeroCount = 0;

            // 32-bit integers are 4 bytes
            const int dataTypeSize = sizeof(int);

            // Calculate TIC and BPI while run length zero encoding
            var i = -1;
            foreach (var intensity in intensities)
            {
                i++;
                if (intensity > 0)
                {
                    // TIC is just the sum of all intensities
                    tic += intensity;
                    if (intensity > bpi)
                    {
                        bpi = intensity;
                        indexOfMaxIntensity = i;
                    }

                    if (zeroCount < 0)
                    {
                        rlzeDataList.Add(zeroCount);
                        zeroCount = 0;
                    }

                    rlzeDataList.Add(intensity);
                    nonZeroCount++;
                }
                else
                {
                    if (zeroCount == int.MinValue)
                    {
                        // Too many zeroes; need to append two points to rlzeDataList to avoid an overflow
                        rlzeDataList.Add(zeroCount);
                        rlzeDataList.Add(0);
                        zeroCount = 0;
                    }

                    zeroCount--;
                }
            }

            // Compress intensities
            var nlzf = 0;
            var nrlze = rlzeDataList.Count;
            var runLengthZeroEncodedData = rlzeDataList.ToArray();

            var compressedData = new byte[nrlze * dataTypeSize];
            if (nrlze > 0)
            {
                var byteBuffer = new byte[nrlze * dataTypeSize];
                Buffer.BlockCopy(runLengthZeroEncodedData, 0, byteBuffer, 0, nrlze * dataTypeSize);
                nlzf = LZFCompressionUtil.Compress(
                    ref byteBuffer,
                    nrlze * dataTypeSize,
                    ref compressedData,
                    compressedData.Length);
            }

            if (nlzf != 0)
            {
                spectra = new byte[nlzf];
                Array.Copy(compressedData, spectra, nlzf);
            }

            return nonZeroCount;

        }

        /// <summary>
        /// Convert an array of intensities to a zero length encoded byte array
        /// </summary>
        /// <param name="intensities"></param>
        /// <param name="spectra"></param>
        /// <param name="tic"></param>
        /// <param name="bpi"></param>
        /// <param name="indexOfMaxIntensity"></param>
        /// <returns>
        /// Number of non-zero data points
        /// </returns>
        public static int Encode(
            this double[] intensities,
            out byte[] spectra,
            out double tic,
            out double bpi,
            out int indexOfMaxIntensity)
        {
            spectra = null;
            tic = 0;
            bpi = 0;
            indexOfMaxIntensity = 0;

            var nonZeroCount = 0;

            // RLZE - convert 0s to negative multiples as well as calculate TIC and BPI, BPI_MZ
            short zeroCount = 0;
            var rlzeDataList = new List<double>();

            // Doubles are 8 bytes
            const int dataTypeSize = 8;

            // Calculate TIC and BPI while run length zero encoding
            for (var i = 0; i < intensities.Length; i++)
            {
                var intensity = intensities[i];
                if (intensity > 0)
                {
                    // TIC is just the sum of all intensities
                    tic += intensity;
                    if (intensity > bpi)
                    {
                        bpi = intensity;
                        indexOfMaxIntensity = i;
                    }

                    if (zeroCount < 0)
                    {
                        rlzeDataList.Add(zeroCount);
                        zeroCount = 0;
                    }

                    rlzeDataList.Add(intensity);
                    nonZeroCount++;
                }
                else
                {
                    if (zeroCount == Int16.MinValue)
                    {
                        // Too many zeroes; need to append two points to rlzeDataList to avoid an overflow
                        rlzeDataList.Add(zeroCount);
                        rlzeDataList.Add((short)0);
                        zeroCount = 0;
                    }

                    zeroCount--;
                }
            }

            // Compress intensities
            var nlzf = 0;
            var nrlze = rlzeDataList.Count;
            var runLengthZeroEncodedData = rlzeDataList.ToArray();

            var compressedData = new byte[nrlze * dataTypeSize * 5];
            if (nrlze > 0)
            {
                var byteBuffer = new byte[nrlze * dataTypeSize];
                Buffer.BlockCopy(runLengthZeroEncodedData, 0, byteBuffer, 0, nrlze * dataTypeSize);
                nlzf = LZFCompressionUtil.Compress(
                    ref byteBuffer,
                    nrlze * dataTypeSize,
                    ref compressedData,
                    compressedData.Length);
            }

            if (nlzf != 0)
            {
                spectra = new byte[nlzf];
                Array.Copy(compressedData, spectra, nlzf);
            }

            return nonZeroCount;

        }

        /// <summary>
        /// Convert an array of intensities to a zero length encoded byte array
        /// </summary>
        /// <param name="intensities"></param>
        /// <param name="spectra"></param>
        /// <param name="tic"></param>
        /// <param name="bpi"></param>
        /// <param name="indexOfMaxIntensity"></param>
        /// <returns>
        /// Number of non-zero data points
        /// </returns>
        public static int Encode(
            this float[] intensities,
            out byte[] spectra,
            out double tic,
            out double bpi,
            out int indexOfMaxIntensity)
        {
            spectra = null;
            tic = 0;
            bpi = 0;
            indexOfMaxIntensity = 0;

            // RLZE - convert 0s to negative multiples as well as calculate TIC and BPI, BPI_MZ
            short zeroCount = 0;
            var rlzeDataList = new List<float>();
            var nonZeroCount = 0;

            // Floats are 4 bytes
            const int dataTypeSize = 4;

            // Calculate TIC and BPI while run length zero encoding
            for (var i = 0; i < intensities.Length; i++)
            {
                var intensity = intensities[i];
                if (intensity > 0)
                {
                    // TIC is just the sum of all intensities
                    tic += intensity;
                    if (intensity > bpi)
                    {
                        bpi = intensity;
                        indexOfMaxIntensity = i;
                    }

                    if (zeroCount < 0)
                    {
                        rlzeDataList.Add(zeroCount);
                        zeroCount = 0;
                    }

                    rlzeDataList.Add(intensity);
                    nonZeroCount++;
                }
                else
                {
                    if (zeroCount == Int16.MinValue)
                    {
                        // Too many zeroes; need to append two points to rlzeDataList to avoid an overflow
                        rlzeDataList.Add(zeroCount);
                        rlzeDataList.Add((short)0);
                        zeroCount = 0;
                    }

                    zeroCount--;
                }
            }

            // Compress intensities
            var nlzf = 0;
            var nrlze = rlzeDataList.Count;
            var runLengthZeroEncodedData = rlzeDataList.ToArray();

            var compressedData = new byte[nrlze * dataTypeSize * 5];
            if (nrlze > 0)
            {
                var byteBuffer = new byte[nrlze * dataTypeSize];
                Buffer.BlockCopy(runLengthZeroEncodedData, 0, byteBuffer, 0, nrlze * dataTypeSize);
                nlzf = LZFCompressionUtil.Compress(
                    ref byteBuffer,
                    nrlze * dataTypeSize,
                    ref compressedData,
                    compressedData.Length);
            }

            if (nlzf != 0)
            {
                spectra = new byte[nlzf];
                Array.Copy(compressedData, spectra, nlzf);
            }

            return nonZeroCount;

        }
    }
}