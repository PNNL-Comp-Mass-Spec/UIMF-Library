using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
    /// <summary>
    /// Convert sets of intensities to run-length zero encoding and compression
    /// </summary>
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
        [Obsolete("Use IntensityConverterCLZF.Compress(IReadOnlyList<int>,...)")]
        public static int Encode(
            IReadOnlyList<int> intensities,
            out byte[] spectra,
            out double tic,
            out double bpi,
            out int indexOfMaxIntensity)
        {
            // 32-bit integers are 4 bytes
            const int dataTypeSize = 4;

            spectra = null;
            var nonZeroCount = RlzEncode.Encode(intensities, out var runLengthZeroEncodedData, out tic, out bpi, out indexOfMaxIntensity);
            var encodedDataLength = runLengthZeroEncodedData.Length;

            if (encodedDataLength > 0)
            {
                spectra = new byte[encodedDataLength * dataTypeSize];
                Buffer.BlockCopy(runLengthZeroEncodedData, 0, spectra, 0, encodedDataLength * dataTypeSize);
                spectra = CLZF2.Compress(spectra);
            }

            return nonZeroCount;
        }

#if (EXPERIMENTAL)
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
            // 32-bit integers are 4 bytes
            const int dataTypeSize = 4;

            spectra = null;
            var nonZeroCount = RlzEncode.Encode(intensities, out var runLengthZeroEncodedData, out tic, out bpi, out indexOfMaxIntensity);
            var encodedDataLength = runLengthZeroEncodedData.Length;

            if (encodedDataLength > 0)
            {
                spectra = new byte[encodedDataLength * dataTypeSize];
                Buffer.BlockCopy(runLengthZeroEncodedData, 0, spectra, 0, encodedDataLength * dataTypeSize);
                spectra = Snappy.SnappyCodec.Compress(spectra);
            }

            return nonZeroCount;
        }
#endif
    }
}
