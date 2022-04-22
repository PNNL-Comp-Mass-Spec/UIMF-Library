// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Defines the SpectrumCache type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System;
using System.Linq;

namespace UIMFLibrary
{
    using System.Collections.Generic;

    /// <summary>
    /// Spectrum cache
    /// </summary>
    public class SpectrumCache
    {
        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="SpectrumCache"/> class.
        /// </summary>
        /// <param name="startFrameNumber">
        /// Start frame number
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number
        /// </param>
        /// <param name="listOfIntensityDictionaries">
        /// List of intensity dictionaries (previously a list of dictionaries, now a list of SortedList objects)
        /// </param>
        /// <param name="summedIntensityDictionary">
        /// Summed intensity dictionary
        /// </param>
        // ReSharper disable once UnusedMember.Global
        public SpectrumCache(
            int startFrameNumber,
            int endFrameNumber,
            IList<SortedList<int, int>> listOfIntensityDictionaries,
            IDictionary<int, int> summedIntensityDictionary)
        {
            StartFrameNumber = startFrameNumber;
            EndFrameNumber = endFrameNumber;
            ListOfIntensityDictionaries = listOfIntensityDictionaries;
            SummedIntensityDictionary = summedIntensityDictionary;

            FindFirstLastScan(listOfIntensityDictionaries, out var firstScan, out var lastScan);

            FirstScan = firstScan;
            LastScan = lastScan;

            UpdateMemoryUsageEstimate();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SpectrumCache"/> class.
        /// </summary>
        /// <param name="startFrameNumber">
        /// Start frame number
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number
        /// </param>
        /// <param name="listOfIntensityDictionaries">
        /// List of intensity dictionaries
        /// </param>
        /// <param name="summedIntensityDictionary">
        /// Summed intensity dictionary
        /// </param>
        /// <param name="firstScan">The scan number of the first entry in listOfIntensityDictionaries that has non-zero intensities</param>
        /// <param name="lastScan">The scan number of the last entry in listOfIntensityDictionaries that has non-zero intensities</param>
        public SpectrumCache(
            int startFrameNumber,
            int endFrameNumber,
            IList<SortedList<int, int>> listOfIntensityDictionaries,
            IDictionary<int, int> summedIntensityDictionary,
            int firstScan,
            int lastScan)
        {
            StartFrameNumber = startFrameNumber;
            EndFrameNumber = endFrameNumber;
            ListOfIntensityDictionaries = listOfIntensityDictionaries;
            SummedIntensityDictionary = summedIntensityDictionary;

            FirstScan = firstScan;
            LastScan = lastScan;

            FindFirstLastScan(listOfIntensityDictionaries, out var firstScanComputed, out var lastScanComputed);

            if (firstScanComputed > 0 || lastScanComputed > 0)
            {
                if (FirstScan < firstScanComputed)
                    FirstScan = firstScanComputed;

                if (LastScan > lastScanComputed)
                    LastScan = lastScanComputed;
            }

            UpdateMemoryUsageEstimate();
        }

        #endregion

        #region Public Properties

        /// <summary>
        /// Gets the end frame number.
        /// </summary>
        public int EndFrameNumber { get; }

        /// <summary>
        /// Gets the list of intensity Lists
        /// </summary>
        /// <remarks>
        /// List of SortedLists tracking the intensity information for scans 0 through NumScans-1 (older .UIMF files) or 1 through NumScans (newer .UIMF files)
        /// Keys in each SortedList are bin number; values are the intensity for the bin
        /// Prior to January 2015 we used a Dictionary(int, int), which gives faster lookups for .TryGetValue
        /// However, a Dictionary uses roughly 2x more memory vs. a SortedList, which can cause problems for rich UIMF files
        /// </remarks>
        public IList<SortedList<int, int>> ListOfIntensityDictionaries { get; }

        /// <summary>
        /// Gets the start frame number.
        /// </summary>
        public int StartFrameNumber { get; }

        /// <summary>
        /// Gets the first (minimum) scan number in ListOfIntensityDictionaries
        /// </summary>
        public int FirstScan { get; }

        /// <summary>
        /// Gets the last (maximum) scan number in ListOfIntensityDictionaries
        /// </summary>
        public int LastScan { get; }

        /// <summary>
        /// Gets the summed intensity dictionary.
        /// </summary>
        public IDictionary<int, int> SummedIntensityDictionary { get; }

        /// <summary>
        /// Estimated MB of data tracked by this cached spectrum
        /// </summary>
        public int MemoryUsageEstimateMB { get; private set; }

        #endregion

        #region Member Functions

        private void FindFirstLastScan(IList<SortedList<int, int>> listOfIntensityDictionaries, out int firstScan, out int lastScan)
        {
            firstScan = int.MaxValue;
            lastScan = int.MinValue;

            for (var i = 0; i < listOfIntensityDictionaries.Count; i++)
            {
                if (listOfIntensityDictionaries[i].Any(dataPoint => dataPoint.Value > 0))
                {
                    firstScan = Math.Min(firstScan, i);
                    lastScan = Math.Max(lastScan, i);
                }
            }

            if (firstScan == int.MaxValue)
            {
                firstScan = 0;
                lastScan = 0;
            }
        }

        /// <summary>
        /// Estimates the amount of memory used by SummedIntensityDictionary and ListOfIntensityDictionaries
        /// </summary>
        /// <remarks>
        /// SummedIntensityDictionary is an int,int dictionary, so each entry takes up 8 bytes.
        /// However, given the overhead inherent in a dictionary, we need to multiply by 5 to get a realistic estimate of the size.
        /// ListOfIntensityDictionaries used to be a list of dictionaries, but it is now a list of SortedList objects.
        /// Each entry nominally takes up 8 bytes, but in reality each entry takes up 16 bytes.</remarks>
        private void UpdateMemoryUsageEstimate()
        {
            Int64 byteEstimate = SummedIntensityDictionary.Count * 8 * 5;
            foreach (var item in ListOfIntensityDictionaries)
            {
                byteEstimate += item.Count * 8 * 2;
            }

            MemoryUsageEstimateMB = (int)(byteEstimate / 1024.0 / 1024.0);
        }

        #endregion
    }
}