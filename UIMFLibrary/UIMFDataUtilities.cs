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

        /// <summary>
        /// Convert a double to a string, forcing invariant culture
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string DoubleToString(double value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Convert a float to a string, forcing invariant culture
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
	    public static string FloatToString(double value)
	    {
	        return FloatToString((float)value);
	    }

        /// <summary>
        /// Convert a double to a string, forcing invariant culture
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string FloatToString(float value)
        {
            if ((value - (int)value) < float.Epsilon * 2)
                return ((int)value).ToString(CultureInfo.InvariantCulture);

            return value.ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Convert an int to a string, forcing invariant culture
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string IntToString(double value)
        {
            return IntToString((int)value);
        }

        /// <summary>
        /// Convert a int to a string, forcing invariant culture
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
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
			var intensityArrLength = yvals.Length;
			var tempIntensities = yvals;
            var targetIndex = 0;

			for (var k = 0; k < intensityArrLength; k++)
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

        /// <summary>
        /// Force date string output to an invariant culture format
        /// </summary>
        /// <param name="dateString"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Force date string output to an invariant culture format
        /// </summary>
        /// <param name="dateValue"></param>
        /// <returns></returns>
        public static string StandardizeDate(DateTime dateValue)
	    {
	        if (dateValue > DateTime.MinValue)
                return dateValue.ToString("yyyy-MM-dd hh:mm:ss tt");

            return string.Empty;
	    }

	    #endregion
	}
}