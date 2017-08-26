using System;
using System.Collections.Generic;
using System.Linq;

namespace UIMFLibrary
{
    internal static class IntensityBinConverterInt32
    {
        /// <summary>
        /// Convert a list of intensity information by bin to a zero length encoded byte array
        /// </summary>
        /// <param name="binToIntensityMap">Keys are bin numbers and values are intensity values; intensity values are assumed to all be non-zero</param>
        /// <param name="timeOffset">Time offset</param>
        /// <param name="spectra">Spectra intensity bytes (output)</param>
        /// <param name="tic">TIC (output)</param>
        /// <param name="bpi">Base peak intensity (output)</param>
        /// <param name="binNumberMaxIntensity">Bin number of the BPI</param>
        /// <returns>
        /// Number of non-zero data points
        /// </returns>
        /// <remarks>
        /// This function assumes that all data in binToIntensityMap has positive (non-zero) intensities
        /// </remarks>
        [Obsolete("Use the version of Encode that takes a list of Tuples")]
        public static int Encode(
            IList<KeyValuePair<int, int>> binToIntensityMap,
            int timeOffset,
            out byte[] spectra,
            out double tic,
            out double bpi,
            out int binNumberMaxIntensity)
        {
            var binToIntensityMapCopy = new List<Tuple<int, int>>(binToIntensityMap.Count);
            binToIntensityMapCopy.AddRange(binToIntensityMap.Select(item => new Tuple<int, int>(item.Key, item.Value)));

            return Encode(binToIntensityMapCopy, timeOffset, out spectra, out tic, out bpi, out binNumberMaxIntensity);
        }

        /// <summary>
        /// Convert a list of intensity information by bin to a zero length encoded byte array
        /// </summary>
        /// <param name="binToIntensityMap">Keys are bin numbers and values are intensity values; intensity values are assumed to all be non-zero</param>
        /// <param name="timeOffset">Time offset</param>
        /// <param name="spectra">Spectra intensity bytes (output)</param>
        /// <param name="tic">TIC (output)</param>
        /// <param name="bpi">Base peak intensity (output)</param>
        /// <param name="binNumberMaxIntensity">Bin number of the BPI</param>
        /// <returns>
        /// Number of non-zero data points
        /// </returns>
        /// <remarks>
        /// This function assumes that all data in binToIntensityMap has positive (non-zero) intensities
        /// </remarks>
        public static int Encode(
            IList<Tuple<int, int>> binToIntensityMap,
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

            // RLZE - convert 0s to negative multiples as well as calculate TIC and BPI, BPI_MZ
            var rlzeDataList = new List<int>();
            var nonZeroCount = 0;

            // 32-bit integers are 4 bytes
            const int dataTypeSize = 4;

            // Calculate TIC and BPI while run length zero encoding
            var previousBin = int.MinValue;

            rlzeDataList.Add(-(timeOffset + binToIntensityMap[0].Item1));
            for (var i = 0; i < binToIntensityMap.Count; i++)
            {
                var intensity = binToIntensityMap[i].Item2;
                var currentBin = binToIntensityMap[i].Item1;

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
                nonZeroCount++;

                previousBin = currentBin;
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
#if NET47
        /// <summary>
        /// Convert a list of intensity information by bin to a zero length encoded byte array
        /// </summary>
        /// <param name="binToIntensityMap">Keys are bin numbers and values are intensity values; intensity values are assumed to all be non-zero</param>
        /// <param name="timeOffset">Time offset</param>
        /// <param name="spectra">Spectra intensity bytes (output)</param>
        /// <param name="tic">TIC (output)</param>
        /// <param name="bpi">Base peak intensity (output)</param>
        /// <param name="binNumberMaxIntensity">Bin number of the BPI</param>
        /// <returns>
        /// Number of non-zero data points
        /// </returns>
        /// <remarks>
        /// This function assumes that all data in binToIntensityMap has positive (non-zero) intensities
        /// </remarks>
        public static int Encode(
            IList<(int, int)> binToIntensityMap,
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

            // RLZE - convert 0s to negative multiples as well as calculate TIC and BPI, BPI_MZ
            var rlzeDataList = new List<int>();
            var nonZeroCount = 0;

            // 32-bit integers are 4 bytes
            const int dataTypeSize = 4;

            // Calculate TIC and BPI while run length zero encoding
            var previousBin = int.MinValue;

            rlzeDataList.Add(-(timeOffset + binToIntensityMap[0].Item1));
            for (var i = 0; i < binToIntensityMap.Count; i++)
            {
                var intensity = binToIntensityMap[i].Item2;
                var currentBin = binToIntensityMap[i].Item1;

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
                nonZeroCount++;

                previousBin = currentBin;
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
#endif

    }
}


