using System;
using System.Collections.Generic;
using System.Text;

namespace UIMFLibrary
{
	public class SpectrumCache
	{
		public int StartFrameNumber { get; private set; }
		public int EndFrameNumber { get; private set; }
		public IList<IDictionary<int, int>> ListOfIntensityDictionries { get; private set; }

		public SpectrumCache(int startFrameNumber, int endFrameNumber, IList<IDictionary<int, int>> listOfIntensityDictionaries)
		{
			this.StartFrameNumber = startFrameNumber;
			this.EndFrameNumber = endFrameNumber;
			this.ListOfIntensityDictionries = listOfIntensityDictionaries;
		}
	}
}
