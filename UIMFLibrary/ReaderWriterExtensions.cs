using System;
using System.Collections.Generic;
using System.Threading.Tasks;

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
        /// <param name="frameNum"></param>
        /// <param name="frameParameters"></param>
        /// <returns></returns>
        public static async Task InsertFrameAsync(this DataWriter dataWriter, int frameNum, Dictionary<FrameParamKeyType, string> frameParameters)
        {
            try
            {
                await Task.Run(() => dataWriter.InsertFrame(frameNum, frameParameters));
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
