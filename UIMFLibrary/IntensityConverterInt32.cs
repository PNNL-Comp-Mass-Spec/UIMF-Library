using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
    public static class IntensityConverterInt32
    {
        /// <summary>
        /// Convert an array of intensities to a zero length encoded byte array
        /// </summary>
        /// <param name="intensities">Array of intensities, including all zeros</param>
        /// <param name="spectra">Spectra intensity bytes (output)</param>
        /// <param name="tic">TIC (output)</param>
        /// <param name="bpi">Base peak intensity (output)</param>
        /// <param name="indexOfMaxIntensity">Index number of the BPI</param>
        /// <returns>
        /// Number of non-zero data points
        /// </returns>
        public static int Encode(
            IList<int> intensities,
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
            var zeroCount = 0;
            var rlzeDataList = new List<int>();
            var nonZeroCount = 0;

            // 32-bit integers are 4 bytes
            const int dataTypeSize = 4;

            // Calculate TIC and BPI while run length zero encoding
            for (var i = 0; i < intensities.Count; i++)
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
                    if (zeroCount == int.MinValue)
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
            var nrlze = rlzeDataList.Count;
            var runLengthZeroEncodedData = rlzeDataList.ToArray();

            if (nrlze > 0)
            {
                spectra = new byte[nrlze * dataTypeSize];
                Buffer.BlockCopy(runLengthZeroEncodedData, 0, spectra, 0, nrlze * dataTypeSize);
                spectra = CLZF2.Compress(
                     spectra);
            }

            return nonZeroCount;

        }

        /// <summary>
        /// Convert an array of intensities to a zero length encoded byte array
        /// </summary>
        /// <param name="intensities">Array of intensities, including all zeros</param>
        /// <param name="spectra">Spectra intensity bytes (output)</param>
        /// <param name="tic">TIC (output)</param>
        /// <param name="bpi">Base peak intensity (output)</param>
        /// <param name="indexOfMaxIntensity">Index number of the BPI</param>
        /// <returns>
        /// Number of non-zero data points
        /// </returns>
        public static int EncodeSnappy(
            IList<int> intensities,
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
            var zeroCount = 0;
            var rlzeDataList = new List<int>();
            var nonZeroCount = 0;

            // 32-bit integers are 4 bytes
            const int dataTypeSize = 4;

            // Calculate TIC and BPI while run length zero encoding
            for (var i = 0; i < intensities.Count; i++)
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
            var nrlze = rlzeDataList.Count;
            var runLengthZeroEncodedData = rlzeDataList.ToArray();

            if (nrlze > 0)
            {
                spectra = new byte[nrlze * dataTypeSize];
                Buffer.BlockCopy(runLengthZeroEncodedData, 0, spectra, 0, nrlze * dataTypeSize);
                spectra = Snappy.SnappyCodec.Compress(spectra);
            }

            return nonZeroCount;

        }

    }
}
