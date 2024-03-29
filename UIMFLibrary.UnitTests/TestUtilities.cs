﻿using System;
using System.Linq;
using System.Text;

namespace UIMFLibrary.UnitTests
{
    /// <summary>
    /// Test utilities
    /// </summary>
    public static class TestUtilities
    {
        // Ignore Spelling: num

        #region Public Methods and Operators

        /// <summary>
        /// Display 2D chromatogram.
        /// </summary>
        /// <param name="frameOrScanValues">Frame or scan values</param>
        /// <param name="intensityValues">Intensity values</param>
        public static void Display2DChromatogram(int[] frameOrScanValues, int[] intensityValues)
        {
            var sb = new StringBuilder();
            for (var i = 0; i < frameOrScanValues.Length; i++)
            {
                sb.Append(frameOrScanValues[i]);
                sb.Append("\t");
                sb.Append(intensityValues[i]);
                sb.Append(Environment.NewLine);
            }

            Console.WriteLine(sb.ToString());
        }

        /// <summary>
        /// Display frame parameters.
        /// </summary>
        /// <param name="frameParams">Frame parameters</param>
        public static void DisplayFrameParameters(FrameParams frameParams)
        {
            var sb = new StringBuilder();

            var separator = Environment.NewLine;

            sb.AppendFormat("avg TOF length = \t{0}", frameParams.GetValueDouble(FrameParamKeyType.AverageTOFLength, 0));
            sb.Append(separator);
            sb.AppendFormat("cal intercept = \t{0}", frameParams.CalibrationIntercept);
            sb.Append(separator);
            sb.AppendFormat("cal slope = \t{0}", frameParams.CalibrationSlope);
            sb.Append(separator);
            sb.AppendFormat("frame type = \t{0}", frameParams.FrameType);
            sb.Append(separator);
            sb.AppendFormat("pressure back = \t{0}", frameParams.GetValueDouble(FrameParamKeyType.PressureBack, 0));
            sb.Append(separator);
            sb.AppendFormat("pressure front = \t{0}", frameParams.GetValueDouble(FrameParamKeyType.PressureFront, 0));
            sb.Append(separator);
            sb.AppendFormat("high pressure funnel pressure = \t{0}", frameParams.GetValueDouble(FrameParamKeyType.HighPressureFunnelPressure, 0));
            sb.Append(separator);
            sb.AppendFormat("ion funnel trap pressure = \t{0}", frameParams.GetValueDouble(FrameParamKeyType.IonFunnelTrapPressure, 0));
            sb.Append(separator);
            sb.AppendFormat("quadrupole pressure = \t{0}", frameParams.GetValueDouble(FrameParamKeyType.QuadrupolePressure, 0));
            sb.Append(separator);
            sb.AppendFormat("rear ion funnel pressure = \t{0}", frameParams.GetValueDouble(FrameParamKeyType.RearIonFunnelPressure, 0));
            sb.Append(separator);
            sb.AppendFormat("start time = \t{0}", frameParams.GetValueDouble(FrameParamKeyType.StartTimeMinutes, 0));
            sb.Append(separator);
            sb.AppendFormat("num scans = \t{0}", frameParams.Scans);
            sb.Append(separator);
            sb.AppendFormat("IMF profile = \t{0}", frameParams.GetValue(FrameParamKeyType.MultiplexingEncodingSequence));

            Console.WriteLine(sb.ToString());
        }

        /// <summary>
        /// Display raw mass spectrum.
        /// </summary>
        /// <param name="mzValues">
        /// m/z values
        /// </param>
        /// <param name="intensities">
        /// Intensities
        /// </param>
        public static void DisplayRawMassSpectrum(double[] mzValues, int[] intensities)
        {
            var sb = new StringBuilder();
            for (var i = 0; i < mzValues.Length; i++)
            {
                sb.Append(mzValues[i]);
                sb.Append("\t");
                sb.Append(intensities[i]);
                sb.Append(Environment.NewLine);
            }

            Console.WriteLine(sb.ToString());
        }

        /// <summary>
        /// Get the max value in a list of integers
        /// </summary>
        /// <param name="values">
        /// Values
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
        /// Get the max value in a 2D array of integers
        /// </summary>
        /// <param name="values">
        /// Values
        /// </param>
        /// <param name="xCoordinate">
        /// X-coordinate of the maximum value
        /// </param>
        /// <param name="yCoordinate">
        /// Y-coordinate of the maximum value
        /// </param>
        /// <returns>
        /// Maximum value<see cref="int"/>.
        /// </returns>
        public static int GetMax(int[][] values, out int xCoordinate, out int yCoordinate)
        {
            var max = 0;
            xCoordinate = 0;
            yCoordinate = 0;

            for (var i = 0; i < values.Length; i++)
            {
                for (var j = 0; j < values[i].Length; j++)
                {
                    if (values[i][j] > max)
                    {
                        max = values[i][j];
                        xCoordinate = i;
                        yCoordinate = j;
                    }
                }
            }

            return max;
        }

        /// <summary>
        /// Print data as a matrix
        /// </summary>
        /// <param name="frameValues">
        /// Frame values
        /// </param>
        /// <param name="intensityValues">
        /// Intensity values
        /// </param>
        /// <param name="cutoff">
        /// Cutoff
        /// </param>
        public static void PrintAsAMatrix(int[] frameValues, float[] intensityValues, float cutoff)
        {
            var sb = new StringBuilder();
            var frameValue = frameValues[0];
            for (var i = 0; i < frameValues.Length; i++)
            {
                if (frameValue != frameValues[i])
                {
                    sb.Append("\n");
                    frameValue = frameValues[i];
                }
                else
                {
                    if (intensityValues[i] < cutoff)
                    {
                        sb.Append("0,");
                    }
                    else
                    {
                        sb.AppendFormat("{0},", intensityValues[i]);
                    }
                }
            }

            Console.WriteLine(sb.ToString());
        }

        /// <summary>
        /// Print data as a matrix
        /// </summary>
        /// <param name="frameValues">
        /// Frame values
        /// </param>
        /// <param name="intensityValues">
        /// Intensity values
        /// </param>
        /// <param name="cutoff">
        /// Cutoff
        /// </param>
        public static void PrintAsAMatrix(int[] frameValues, int[] intensityValues, float cutoff)
        {
            var sb = new StringBuilder();
            var frameValue = frameValues[0];
            for (var i = 0; i < frameValues.Length; i++)
            {
                if (frameValue != frameValues[i])
                {
                    sb.Append("\n");
                    frameValue = frameValues[i];
                }
                else
                {
                    if (intensityValues[i] < cutoff)
                    {
                        sb.Append("0,");
                    }
                    else
                    {
                        sb.AppendFormat("{0},", intensityValues[i]);
                    }
                }
            }

            Console.WriteLine(sb.ToString());
        }

        #endregion
    }
}