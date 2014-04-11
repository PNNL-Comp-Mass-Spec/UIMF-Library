// --------------------------------------------------------------------------------------------------------------------
// <copyright file="SpectrumCache.cs" company="">
//   
// </copyright>
// <summary>
//   Defines the SpectrumCache type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary
{
	using System.Collections.Generic;

	/// <summary>
	/// TODO The spectrum cache.
	/// </summary>
	public class SpectrumCache
	{
		public int StartFrameNumber { get; private set; }
		public int EndFrameNumber { get; private set; }
		public IList<IDictionary<int, int>> ListOfIntensityDictionaries { get; private set; }
		public IDictionary<int, int> SummedIntensityDictionary { get; private set; }

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
		public SpectrumCache(int startFrameNumber, int endFrameNumber, IList<IDictionary<int, int>> listOfIntensityDictionaries, IDictionary<int, int> summedIntensityDictionary)
		{
			this.StartFrameNumber = startFrameNumber;
			this.EndFrameNumber = endFrameNumber;
			this.ListOfIntensityDictionaries = listOfIntensityDictionaries;
			this.SummedIntensityDictionary = summedIntensityDictionary;
		}
	}
}
