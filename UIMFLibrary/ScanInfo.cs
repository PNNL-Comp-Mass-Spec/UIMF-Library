// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Tracks information for a given scan
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary
{
    /// <summary>
    /// Class for tracking pre-computed information about a given scan
    /// </summary>
    public class ScanInfo
    {
        private readonly int mFrameNumber;
        private readonly int mScanNumber;

        /// <summary>
        /// Frame number for scan
        /// </summary>
        public int Frame 
        {
            get
            {
                return mFrameNumber;
            }
        }

        /// <summary>
        /// Scan number
        /// </summary>
        public int Scan
        {
            get
            {
                return mScanNumber;
            }
        }

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
            mFrameNumber = frameNumber;
            mScanNumber = scanNumber;
        }
    }
}
