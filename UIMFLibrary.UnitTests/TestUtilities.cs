// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Test utilities.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System.Linq;

namespace UIMFLibrary.UnitTests
{
	using System;
	using System.Text;

	/// <summary>
	/// The test utilities.
	/// </summary>
	public class TestUtilities
	{
		#region Public Methods and Operators

		/// <summary>
		/// Display 2D chromatogram.
		/// </summary>
		/// <param name="frameORScanVals">
		/// The frame or scan vals.
		/// </param>
		/// <param name="intensityVals">
		/// The intensity vals.
		/// </param>
		public static void Display2DChromatogram(int[] frameORScanVals, int[] intensityVals)
		{
			var sb = new StringBuilder();
			for (int i = 0; i < frameORScanVals.Length; i++)
			{
				sb.Append(frameORScanVals[i]);
				sb.Append("\t");
				sb.Append(intensityVals[i]);
				sb.Append(Environment.NewLine);
			}

			Console.WriteLine(sb.ToString());
		}

		/// <summary>
		/// Display frame parameters.
		/// </summary>
		/// <param name="fp">
		/// The fp.
		/// </param>
		public static void DisplayFrameParameters(FrameParams fp)
		{
			var sb = new StringBuilder();

			string separator = Environment.NewLine;

            sb.Append("avg TOF length = \t" + fp.GetValueDouble(FrameParamKeyType.AverageTOFLength, 0));
			sb.Append(separator);
			sb.Append("cal intercept = \t" + fp.CalibrationIntercept);
			sb.Append(separator);
			sb.Append("cal slope = \t" + fp.CalibrationSlope);
			sb.Append(separator);
			sb.Append("frame type = \t" + fp.FrameType);
			sb.Append(separator);
            sb.Append("pressure back = \t" + fp.GetValueDouble(FrameParamKeyType.PressureBack, 0));
			sb.Append(separator);
            sb.Append("pressure front = \t" + fp.GetValueDouble(FrameParamKeyType.PressureFront, 0));
			sb.Append(separator);
            sb.Append("high pressure funnel pressure= \t" + fp.GetValueDouble(FrameParamKeyType.HighPressureFunnelPressure, 0));
			sb.Append(separator);
            sb.Append("ion funnel trap pressure= \t" + fp.GetValueDouble(FrameParamKeyType.IonFunnelTrapPressure, 0));
			sb.Append(separator);
            sb.Append("quadrupole pressure = \t" + fp.GetValueDouble(FrameParamKeyType.QuadrupolePressure, 0));
			sb.Append(separator);
            sb.Append("rear ion funnel pressure = \t" + fp.GetValueDouble(FrameParamKeyType.RearIonFunnelPressure, 0));
			sb.Append(separator);
            sb.Append("start time = \t" + fp.GetValueDouble(FrameParamKeyType.StartTimeMinutes, 0));
			sb.Append(separator);
			sb.Append("num scans = \t" + fp.Scans);
			sb.Append(separator);
            sb.Append("IMF profile = \t" + fp.GetValue(FrameParamKeyType.MultiplexingEncodingSequence));

			Console.WriteLine(sb.ToString());
		}

		/// <summary>
		/// Display raw mass spectrum.
		/// </summary>
		/// <param name="mzValues">
		/// The mz values.
		/// </param>
		/// <param name="intensities">
		/// The intensities.
		/// </param>
		public static void DisplayRawMassSpectrum(double[] mzValues, int[] intensities)
		{
			var sb = new StringBuilder();
			for (int i = 0; i < mzValues.Length; i++)
			{
				sb.Append(mzValues[i]);
				sb.Append("\t");
				sb.Append(intensities[i]);
				sb.Append(Environment.NewLine);
			}

			Console.WriteLine(sb.ToString());
		}

		/// <summary>
		/// Get the max value in a list of ints
		/// </summary>
		/// <param name="values">
		/// The values.
		/// </param>
		/// <returns>
		/// Maximum value<see cref="int"/>.
		/// </returns>
		public static int GetMax(int[] values)
		{
		    var max = (from item in values select item).ToList().Max();
			return max;
		}

		/// <summary>
		/// Get the max value in a 2D array of ints
		/// </summary>
		/// <param name="values">
		/// The values.
		/// </param>
		/// <param name="xcoord">
		/// The xcoord of the maximum value
		/// </param>
		/// <param name="ycoord">
		/// The ycoord of the maximum value
		/// </param>
		/// <returns>
		/// Maximum value<see cref="int"/>.
		/// </returns>
		public static int GetMax(int[][] values, out int xcoord, out int ycoord)
		{
			int max = 0;
			xcoord = 0;
			ycoord = 0;

			for (int i = 0; i < values.Length; i++)
			{
				for (int j = 0; j < values[i].Length; j++)
				{
					if (values[i][j] > max)
					{
						max = values[i][j];
						xcoord = i;
						ycoord = j;
					}
				}
			}

			return max;
		}

		/// <summary>
		/// Print data as a matrix
		/// </summary>
		/// <param name="frameVals">
		/// The frame vals.
		/// </param>
		/// <param name="intensityVals">
		/// The intensity vals.
		/// </param>
		/// <param name="cutoff">
		/// The cutoff.
		/// </param>
		public static void PrintAsAMatrix(int[] frameVals, float[] intensityVals, float cutoff)
		{
			var sb = new StringBuilder();
			int frameValue = frameVals[0];
			for (int i = 0; i < frameVals.Length; i++)
			{
				if (frameValue != frameVals[i])
				{
					sb.Append("\n");
					frameValue = frameVals[i];
				}
				else
				{
					if (intensityVals[i] < cutoff)
					{
						sb.Append("0,");
					}
					else
					{
						sb.Append(intensityVals[i] + ",");
					}
				}
			}

			Console.WriteLine(sb.ToString());
		}

		/// <summary>
		/// Print data as a matrix
		/// </summary>
		/// <param name="frameVals">
		/// The frame vals.
		/// </param>
		/// <param name="intensityVals">
		/// The intensity vals.
		/// </param>
		/// <param name="cutoff">
		/// The cutoff.
		/// </param>
		public static void PrintAsAMatrix(int[] frameVals, int[] intensityVals, float cutoff)
		{
			var sb = new StringBuilder();
			int frameValue = frameVals[0];
			for (int i = 0; i < frameVals.Length; i++)
			{
				if (frameValue != frameVals[i])
				{
					sb.Append("\n");
					frameValue = frameVals[i];
				}
				else
				{
					if (intensityVals[i] < cutoff)
					{
						sb.Append("0,");
					}
					else
					{
						sb.Append(intensityVals[i] + ",");
					}
				}
			}

			Console.WriteLine(sb.ToString());
		}

		#endregion
	}
}