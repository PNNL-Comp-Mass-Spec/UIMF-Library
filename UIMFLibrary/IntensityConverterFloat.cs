using System;

namespace UIMFLibrary
{
    static class IntensityConverterFloat
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
        [Obsolete("Use IntensityConverterCLZF.Compress(IReadOnlyList<float>,...)")]
        public static int Encode(
            float[] intensities,
            out byte[] spectra,
            out double tic,
            out double bpi,
            out int indexOfMaxIntensity)
        {
            // Floats are 4 bytes
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
    }
}
