using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
    static class IntensityConverterDouble
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
        public static int Encode(
            double[] intensities,
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

            return nonZeroCount;
        }
    }
}
