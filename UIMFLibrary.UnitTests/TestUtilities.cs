using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace UIMFLibrary.UnitTests
{
    public class TestUtilities
    {

        public static void displayRawMassSpectrum(double[] mzValues, int[] intensities)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < mzValues.Length; i++)
            {
                sb.Append(mzValues[i]);
                sb.Append("\t");
                sb.Append(intensities[i]);
                sb.Append(Environment.NewLine);


            }

            Console.WriteLine(sb.ToString());
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




        public static void display2DChromatogram(int[] frameORScanVals, int[] intensityVals)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < frameORScanVals.Length; i++)
            {
                sb.Append(frameORScanVals[i]);
                sb.Append("\t");
                sb.Append(intensityVals[i]);
                sb.Append(Environment.NewLine);


            }

            Console.WriteLine(sb.ToString());
        }
    }
}
