// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Tracks information for a given scan
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary
{
    /// <summary>
    /// Class for tracking precomputed information about a given scan
    /// </summary>
    public class ScanInfo
    {
        /// <summary>
        /// Frame number for scan
        /// </summary>
        public int Frame { get; }

        /// <summary>
        /// Scan number
        /// </summary>
        public int Scan { get; }

        /// <summary>
        /// Number of non-zero (positive) intensity values in the scan
        /// </summary>
        public int NonZeroCount { get; set; }

        /// <summary>
        /// Base peak intensity (highest intensity)
        /// </summary>
        public double BPI { get; set; }

        /// <summary>
        /// m/z of the base peak (the data point with the highest intensity)
        /// </summary>
        public double BPI_MZ { get; set; }

        /// <summary>
        /// Total Ion Current (sum of all intensities)
        /// </summary>
        public double TIC { get; set; }

        /// <summary>
        /// Drift time of data in this scan (normalized for pressure)
        /// </summary>
        public double DriftTime { get; set; }

        /// <summary>
        /// Drift time of data in this scan (not normalized for pressure)
        /// </summary>
        public double DriftTimeUnnormalized { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="scanNumber">Scan number</param>
        public ScanInfo(int frameNumber, int scanNumber)
        {
            Frame = frameNumber;
            Scan = scanNumber;
        }

        /// <summary>
        /// Summarize the data tracked by this class
        /// </summary>
        public override string ToString()
        {
            return string.Format("Frame {0}, scan {1}, TIC {2:F0}, BPI {3:F0}, BPI_Mz {4:F4}", Frame, Scan, TIC, BPI, BPI_MZ);
        }
    }
}
