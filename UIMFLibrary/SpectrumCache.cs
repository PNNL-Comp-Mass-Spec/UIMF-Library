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
    /// The spectrum cache.
    /// </summary>
    public class SpectrumCache
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="SpectrumCache"/> class.
        /// </summary>
        /// <param name="startFrameNumber">
        /// The start frame number.
        /// </param>
        /// <param name="endFrameNumber">
        /// The end frame number.
        /// </param>
        /// <param name="listOfIntensityDictionaries">
        /// The list of intensity dictionaries (previously a list of dictionaries, now a list of SortedList objects)
        /// </param>
        /// <param name="summedIntensityDictionary">
        /// The summed intensity dictionary.
        /// </param>
        public SpectrumCache(
            int startFrameNumber,
            int endFrameNumber,
            IList<SortedList<int, int>> listOfIntensityDictionaries, 
            IDictionary<int, int> summedIntensityDictionary)
        {
            this.StartFrameNumber = startFrameNumber;
            this.EndFrameNumber = endFrameNumber;
            this.ListOfIntensityDictionaries = listOfIntensityDictionaries;
            this.SummedIntensityDictionary = summedIntensityDictionary;


            FindFirstLastScan(listOfIntensityDictionaries, out var firstScan, out var lastScan);

            this.FirstScan = firstScan;
            this.LastScan = lastScan;
            
            UpdateMemoryUsageEstimate();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SpectrumCache"/> class.
        /// </summary>
        /// <param name="startFrameNumber">
        /// The start frame number.
        /// </param>
        /// <param name="endFrameNumber">
        /// The end frame number.
        /// </param>
        /// <param name="listOfIntensityDictionaries">
        /// The list of intensity dictionaries.
        /// </param>
        /// <param name="summedIntensityDictionary">
        /// The summed intensity dictionary.
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
            this.StartFrameNumber = startFrameNumber;
            this.EndFrameNumber = endFrameNumber;
            this.ListOfIntensityDictionaries = listOfIntensityDictionaries;
            this.SummedIntensityDictionary = summedIntensityDictionary;

            this.FirstScan = firstScan;
            this.LastScan = lastScan;

            FindFirstLastScan(listOfIntensityDictionaries, out var firstScanComputed, out var lastScanComputed);

            if (firstScanComputed > 0 || lastScanComputed > 0)
            {
                if (this.FirstScan < firstScanComputed)
                    this.FirstScan = firstScanComputed;

                if (this.LastScan > lastScanComputed)
                    this.LastScan = lastScanComputed;
            }

            UpdateMemoryUsageEstimate();
        }
    
        #endregion

        #region Public Properties

        /// <summary>
        /// Gets the end frame number.
        /// </summary>
        public int EndFrameNumber { get; private set; }

        /// <summary>
        /// Gets the list of intensity Lists
        /// </summary>
        /// <remarks>
        /// List of SortedLists tracking the intensity information for scans 0 through NumScans-1 (older .UIMF files) or 1 through NumScans (newer .UIMF files)
        /// Keys in each SortedList are bin number; values are the intensity for the bin
        /// Prior to January 2015 we used a Dictionary(int, int), which gives faster lookups for .TryGetValue
        /// However, a Dictionary uses roughly 2x more memory vs. a SortedList, which can cause problems for rich UIMF files
        /// </remarks>
        public IList<SortedList<int, int>> ListOfIntensityDictionaries { get; private set; }

        /// <summary>
        /// Gets the start frame number.
        /// </summary>
        public int StartFrameNumber { get; private set; }

        /// <summary>
        /// Gets the first (minimum) scan number in ListOfIntensityDictionaries
        /// </summary>
        public int FirstScan { get; private set; }

        /// <summary>
        /// Gets the last (maximum) scan number in ListOfIntensityDictionaries
        /// </summary>
        public int LastScan { get; private set; }

        /// <summary>
        /// Gets the summed intensity dictionary.
        /// </summary>
        public IDictionary<int, int> SummedIntensityDictionary { get; private set; }

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
        /// However, given the overhead inerhent in a dictionary, we need to multiply by 5 to get a realistic estimate of the size.
        /// ListOfIntensityDictionaries used to be a list of dictionaries, but it is now a list of SortedList objects.
        /// Each entry nominally takes up 8 bytes, but in reality each entry takes up 16 bytes.</remarks>
        private void UpdateMemoryUsageEstimate()
        {
            System.Int64 byteEstimate = SummedIntensityDictionary.Count * 8 * 5;
            foreach (var item in ListOfIntensityDictionaries)
            {
                byteEstimate += item.Count * 8 * 2;
            }

            this.MemoryUsageEstimateMB = (int)(byteEstimate / 1024.0 / 1024.0);
        }

        #endregion
    }
}