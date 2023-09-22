using System;
using System.Collections.Generic;
using System.Threading.Tasks;

// ReSharper disable UnusedMember.Global

namespace UIMFLibrary
{
    /// <summary>
    /// Extension methods that use functionality specific to .NET 4.5 and above
    /// </summary>
    public static class ReaderWriterExtensions
    {
        /// <summary>
        /// Asynchronously insert a frame
        /// </summary>
        /// <param name="dataWriter"></param>
        /// <param name="frameNumber"></param>
        /// <param name="frameParameters"></param>
        /// <returns>
        /// Instance of the DataWriter class, which allows for chaining function calls (see https://en.wikipedia.org/wiki/Fluent_interface)
        /// </returns>
        public static async Task InsertFrameAsync(this DataWriter dataWriter, int frameNumber, Dictionary<FrameParamKeyType, dynamic> frameParameters)
        {
            try
            {
                await Task.Run(() => dataWriter.InsertFrame(frameNumber, frameParameters)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ReportError("InsertFrame error: " + ex.Message, ex);
            }
        }

        private static void ReportError(string errorMessage, Exception ex)
        {
            Console.WriteLine(errorMessage);
            throw new Exception(errorMessage, ex);
        }
    }
}
