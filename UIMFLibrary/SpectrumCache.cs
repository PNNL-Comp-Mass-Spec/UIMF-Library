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
		/// The list of intensity dictionaries.
		/// </param>
		/// <param name="summedIntensityDictionary">
		/// The summed intensity dictionary.
		/// </param>
	    public SpectrumCache(
	        int startFrameNumber,
	        int endFrameNumber,
	        IList<IDictionary<int, int>> listOfIntensityDictionaries,
	        IDictionary<int, int> summedIntensityDictionary)
	    {
            this.StartFrameNumber = startFrameNumber;
            this.EndFrameNumber = endFrameNumber;
            this.ListOfIntensityDictionaries = listOfIntensityDictionaries;
            this.SummedIntensityDictionary = summedIntensityDictionary;

            int firstScan;
            int lastScan;

            FindFirstLastScan(listOfIntensityDictionaries, out firstScan, out lastScan);

            this.FirstScan = firstScan;
            this.LastScan = lastScan;
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
			IList<IDictionary<int, int>> listOfIntensityDictionaries, 
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

	        int firstScanComputed;
	        int lastScanComputed;
            FindFirstLastScan(listOfIntensityDictionaries, out firstScanComputed, out lastScanComputed);

	        if (firstScanComputed > 0 || lastScanComputed > 0)
	        {
	            if (this.FirstScan < firstScanComputed)
	                this.FirstScan = firstScanComputed;

                if (this.LastScan > lastScanComputed)
                    this.LastScan = lastScanComputed;
	        }

		}
    
	    #endregion

		#region Public Properties

		/// <summary>
		/// Gets the end frame number.
		/// </summary>
		public int EndFrameNumber { get; private set; }

		/// <summary>
		/// Gets the list of intensity dictionaries.
		/// </summary>
        /// <remarks>
        /// List of dictionaries tracking the intensity information for scans 0 through NumScans-1 (older .UIMF files) or 1 through NumScans (newer .UIMF files)
        /// Keys in each dictionary are bin number; values are the intensity for the bin
        /// </remarks>
		public IList<IDictionary<int, int>> ListOfIntensityDictionaries { get; private set; }

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

		#endregion

        #region Member Functions

        private void FindFirstLastScan(IList<IDictionary<int, int>> listOfIntensityDictionaries, out int firstScan, out int lastScan)
        {

            firstScan = int.MaxValue;
            lastScan = int.MinValue;

            for (int i = 0; i < listOfIntensityDictionaries.Count; i++)
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

        #endregion
    }
}