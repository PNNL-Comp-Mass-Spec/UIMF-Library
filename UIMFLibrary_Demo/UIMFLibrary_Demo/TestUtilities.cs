using System;
using System.Text;
using UIMFLibrary;

namespace UIMFLibrary_Demo
{
    public class TestUtilities
    {

        public static string displayRawMassSpectrum(double[] mzValues, int[] intensities)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < mzValues.Length; i++)
            {
                sb.Append(mzValues[i]);
                sb.Append("\t");
                sb.Append(intensities[i]);
                sb.Append(Environment.NewLine);


            }

            return sb.ToString();
        }


        public static string display2DChromatogram(int[] frameORScanVals, int[] intensityVals)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < frameORScanVals.Length; i++)
            {
                sb.Append(frameORScanVals[i]);
                sb.Append("\t");
                sb.Append(intensityVals[i]);
                sb.Append(Environment.NewLine);


            }

            return sb.ToString();
        }


        public static int getMax(int[] values)
        {
            int max = 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] > max)
                {
                    max = values[i];
                }

            }
            return max;
        }


        public static int getMax(int[][] values, out int xcoord, out int ycoord)
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

        public static void printAsAMatrix(int[] frameVals, float[] intensityVals, float cutoff)
        {
            StringBuilder sb = new StringBuilder();
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

        public static void printAsAMatrix(int[] frameVals, int[] intensityVals, float cutoff)
        {
            StringBuilder sb = new StringBuilder();
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


        public static string FrameParametersToString(FrameParameters fp)
        {
            var sb = new StringBuilder();

            string separator = Environment.NewLine;

            sb.Append("avg TOF length = \t"+ fp.AverageTOFLength);
            sb.Append(separator);
            sb.Append("cal intercept = \t" + fp.CalibrationIntercept);
            sb.Append(separator);
            sb.Append("cal slope = \t" + fp.CalibrationSlope);
            sb.Append(separator);
            sb.Append("frame type = \t" + fp.FrameType);
            sb.Append(separator);
            sb.Append("pressure back = \t" + fp.PressureBack);
            sb.Append(separator);
            sb.Append("pressure front = \t" + fp.PressureFront);
            sb.Append(separator);
            sb.Append("high pressure funnel pressure= \t" + fp.HighPressureFunnelPressure);
            sb.Append(separator);
            sb.Append("ion funnel trap pressure= \t" + fp.IonFunnelTrapPressure);
            sb.Append(separator);
            sb.Append("quadrupole pressure = \t" + fp.QuadrupolePressure);
            sb.Append(separator);
            sb.Append("rear ion funnel pressure = \t" + fp.RearIonFunnelPressure);
            sb.Append(separator);
            sb.Append("start time = \t" + fp.StartTime);
            sb.Append(separator);
            sb.Append("num scans = \t" + fp.Scans);
            sb.Append(separator);
            sb.Append("IMF profile = \t" + fp.IMFProfile);

            return sb.ToString();
        }

        public static string FrameParametersToString(FrameParams frameParams)
        {
            var sb = new StringBuilder();

            string separator = Environment.NewLine;

            sb.Append("avg TOF length = \t" + frameParams.GetValueDouble(FrameParamKeyType.AverageTOFLength));
            sb.Append(separator);
            sb.Append("cal intercept = \t" + frameParams.GetValueDouble(FrameParamKeyType.CalibrationIntercept));
            sb.Append(separator);
            sb.Append("cal slope = \t" + frameParams.GetValueDouble(FrameParamKeyType.CalibrationSlope));
            sb.Append(separator);
            sb.Append("frame type = \t" + frameParams.GetValueDouble(FrameParamKeyType.FrameType));
            sb.Append(separator);
            sb.Append("pressure back = \t" + frameParams.GetValueDouble(FrameParamKeyType.PressureBack));
            sb.Append(separator);
            sb.Append("pressure front = \t" + frameParams.GetValueDouble(FrameParamKeyType.PressureFront));
            sb.Append(separator);
            sb.Append("high pressure funnel pressure= \t" + frameParams.GetValueDouble(FrameParamKeyType.HighPressureFunnelPressure));
            sb.Append(separator);
            sb.Append("ion funnel trap pressure= \t" + frameParams.GetValueDouble(FrameParamKeyType.IonFunnelTrapPressure));
            sb.Append(separator);
            sb.Append("quadrupole pressure = \t" + frameParams.GetValueDouble(FrameParamKeyType.QuadrupolePressure));
            sb.Append(separator);
            sb.Append("rear ion funnel pressure = \t" + frameParams.GetValueDouble(FrameParamKeyType.RearIonFunnelPressure));
            sb.Append(separator);
            sb.Append("start time = \t" + frameParams.GetValueDouble(FrameParamKeyType.StartTimeMinutes));
            sb.Append(separator);
            sb.Append("num scans = \t" + frameParams.GetValueInt32(FrameParamKeyType.Scans));
            sb.Append(separator);
            sb.Append("IMF profile = \t" + frameParams.GetValue(FrameParamKeyType.MultiplexingEncodingSequence));

            return sb.ToString();

        }



        public static string Display3DChromatogram(int[] frameVals, int[] scanVals, int[] intensityVals)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < frameVals.Length; i++)
            {
                sb.Append(frameVals[i] + "\t" + scanVals[i] + "\t" + intensityVals[i] + Environment.NewLine);

            }

            return sb.ToString();
        }
    }
}
