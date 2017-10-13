using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
    /// <summary>
    /// Utilities for encoding intensity using run length encoding
    /// </summary>
    public static class IntensityEncoderUtilities
    {
        /// <summary>
        /// Encode the list of intensity values using run length encoding
        /// </summary>
        /// <param name="intensities">Intensities</param>
        /// <param name="spectra">Encoded intensities, as bytes</param>
        /// <param name="tic">Sum of all intensities</param>
        /// <param name="bpi">Largest intensity</param>
        /// <param name="indexOfMaxIntensity">Data index for the BPI</param>
        /// <param name="nonZeroCount">Number of non-zero values in intensities</param>
        public static void Encode(
            this short[] intensities,
            out byte[] spectra,
            out double tic,
            out double bpi,
            out int indexOfMaxIntensity, out int nonZeroCount)
        {
            spectra = null;
            tic = 0;
            bpi = 0;
            indexOfMaxIntensity = 0;

            // RLZE - convert 0s to negative multiples as well as calculate TIC and BPI, BPI_MZ
            short zeroCount = 0;
            var rlzeDataList = new List<short>();
            nonZeroCount = 0;

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
                    if (zeroCount == short.MinValue)
                    {
                        // Too many zeroes; append the current count to rlzeDataList and reset the count to avoid an underflow
                        rlzeDataList.Add(zeroCount);
                        zeroCount = 0;
                    }

                    // Always count the zero
                    zeroCount--;
                }
            }
            // We don't care about any zeroes/zeroCount after the last non-zero value; it's better if we don't append them to rlzeDataList.

            // Compress intensities
            var nrlze = rlzeDataList.Count;
            var runLengthZeroEncodedData = rlzeDataList.ToArray();

            if (nrlze > 0)
            {
                spectra = new byte[nrlze * dataTypeSize];
                Buffer.BlockCopy(runLengthZeroEncodedData, 0, spectra, 0, nrlze * dataTypeSize);
                spectra = CLZF2.Compress(spectra);
            }
        }
    }
}
