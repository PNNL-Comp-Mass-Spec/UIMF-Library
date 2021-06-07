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
        [Obsolete("Use IntensityConverterCLZF.Compress(IReadOnlyList<short>,...)")]
        public static void Encode(
            this short[] intensities,
            out byte[] spectra,
            out double tic,
            out double bpi,
            out int indexOfMaxIntensity, out int nonZeroCount)
        {
            // 16-bit integers are 2 bytes
            const int dataTypeSize = 2;

            spectra = null;

            nonZeroCount = RlzEncode.Encode(intensities, out var runLengthZeroEncodedData, out tic, out bpi, out indexOfMaxIntensity);

            // Compress intensities
            var encodedDataLength = runLengthZeroEncodedData.Length;

            if (encodedDataLength > 0)
            {
                spectra = new byte[encodedDataLength * dataTypeSize];
                Buffer.BlockCopy(runLengthZeroEncodedData, 0, spectra, 0, encodedDataLength * dataTypeSize);
                spectra = CLZF2.Compress(spectra);
            }
        }
    }
}
