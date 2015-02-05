// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Defines the UIMFDataUtilities type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System.Globalization;

namespace UIMFLibrary
{
	using System;

	/// <summary>
	/// The uimf data utilities.
	/// </summary>
	public class UIMFDataUtilities
	{
		#region Public Methods and Operators

        public static string DoubleToString(double value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }

	    public static string FloatToString(double value)
	    {
	        return FloatToString((float)value);
	    }

        public static string FloatToString(float value)
        {
            if ((value - (int)value) < float.Epsilon * 2)
                return ((int)value).ToString(CultureInfo.InvariantCulture);

            return value.ToString(CultureInfo.InvariantCulture);
        }

        public static string IntToString(double value)
        {
            return IntToString((int)value);
        }

        public static string IntToString(int value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }


	    /// <summary>
        /// Filters xvals and yvals to only contain data with mass between 1 and 100000 m/z, and with intensity > 0
		/// </summary>
		/// <param name="xvals">
		/// The xvals.
		/// </param>
		/// <param name="yvals">
		/// The yvals.
		/// </param>
		public static void ParseOutZeroValues(ref double[] xvals, ref int[] yvals)
		{
			ParseOutZeroValues(ref xvals, ref yvals, 1, 100000);
		}

		/// <summary>
		/// Filters xvals and yvals to only contain data with mass between minMZ and maxMZ, and with intensity > 0
		/// </summary>
		/// <param name="xvals">
		/// The xvals.
		/// </param>
		/// <param name="yvals">
		/// The yvals.
		/// </param>
		/// <param name="minMZ">
		/// The min mz.
		/// </param>
		/// <param name="maxMZ">
		/// The max mz.
		/// </param>
		public static void ParseOutZeroValues(ref double[] xvals, ref int[] yvals, double minMZ, double maxMZ)
		{
			int intensityArrLength = yvals.Length;
			int[] tempIntensities = yvals;
            int targetIndex = 0;

			for (int k = 0; k < intensityArrLength; k++)
			{
				if (tempIntensities[k] > 0 && (minMZ <= xvals[k] && maxMZ >= xvals[k]))
				{
                    xvals[targetIndex] = xvals[k];
                    yvals[targetIndex] = tempIntensities[k];
				    targetIndex++;
				}
			}

			// resize arrays cutting off the zeroes at the end.
            Array.Resize(ref xvals, targetIndex);
            Array.Resize(ref yvals, targetIndex);
		}

        public static string StandardizeDate(string dateString)
        {
            if (string.IsNullOrWhiteSpace(dateString))
                return string.Empty;

            DateTime dateValue;
            if (DateTime.TryParse(dateString, out dateValue))
            {
                return StandardizeDate(dateValue);
            }

            throw new ArgumentException("dateString parameter is not a valid date");
        }

        public static string StandardizeDate(DateTime dateValue)
	    {
	        if (dateValue > DateTime.MinValue)
                return dateValue.ToString("yyyy-MM-dd hh:mm:ss tt");

            return string.Empty;
	    }

	    #endregion
	}
}