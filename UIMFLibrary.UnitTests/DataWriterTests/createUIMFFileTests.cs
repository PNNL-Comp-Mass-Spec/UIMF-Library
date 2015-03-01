// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Create uimf file tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System;
using System.IO;

namespace UIMFLibrary.UnitTests.DataWriterTests
{
    using NUnit.Framework;

    /// <summary>
    /// The create uimf file tests.
    /// </summary>
    [TestFixture]
    public class createUIMFFileTests
    {
        [Test]
        public void createDB_test1()
        {
            DataReaderTests.DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            var fiTarget = new FileInfo(FileRefs.uimfWriterTest10Frames);

            if (fiTarget.Exists)
            {
                fiTarget.Delete();
            }

            using (var writer = new DataWriter(fiTarget.FullName))
            {
                writer.CreateTables("int");

                var globalParameters = new GlobalParams();

                globalParameters.AddUpdateValue(GlobalParamKeyType.Bins, 400000);
                globalParameters.AddUpdateValue(GlobalParamKeyType.BinWidth, 0.25);

                globalParameters.AddUpdateValue(GlobalParamKeyType.DateStarted, DateTime.Now);
                globalParameters.AddUpdateValue(GlobalParamKeyType.NumFrames, 10);
                globalParameters.AddUpdateValue(GlobalParamKeyType.TOFIntensityType, "ADC");

                writer.InsertGlobal(globalParameters);
                writer.AddUpdateGlobalParameter(GlobalParamKeyType.TimeOffset, 1);
                writer.AddUpdateGlobalParameter(GlobalParamKeyType.InstrumentName, "IMS_Test");


                Console.WriteLine("Adding frame 1");

                const float SECONDS_PER_FRAME = 1.25f;

                var randGenerator = new Random();

                for (int frameNum = 1; frameNum <= 10; frameNum++)
                {                   

                    var fp = new FrameParams();

                    fp.AddUpdateValue(FrameParamKeyType.FrameType, (int)DataReader.FrameType.MS1);
                    fp.AddUpdateValue(FrameParamKeyType.CalibrationSlope, 0.3476349957054481);
                    fp.AddUpdateValue(FrameParamKeyType.CalibrationIntercept, 0.03434148864746093);
                    fp.AddUpdateValue(FrameParamKeyType.AverageTOFLength, 163366.6666666667);
                    fp.AddUpdateValue(FrameParamKeyType.StartTimeMinutes, frameNum * SECONDS_PER_FRAME);
                    fp.AddUpdateValue(FrameParamKeyType.Scans, 600);

                    writer.InsertFrame(frameNum, fp);

                    for (int scanNumber = 1; scanNumber <= 600; scanNumber++)
                    {
                        if (scanNumber == 1 | scanNumber % 100 == 0)
                            Console.WriteLine("Adding frame " + frameNum + ", scan " + scanNumber);

                        var intensities = new int[148000];

                        for (int i = 0; i < intensities.Length; i++)
                        {
                            int nextRandom = randGenerator.Next(0, 255);
                            if (nextRandom < 250)
                                intensities[i] = 0;
                            else
                                intensities[i] = nextRandom;

                        }

                        writer.InsertScan(frameNum, fp, scanNumber, intensities, globalParameters.BinWidth);
                    }

                }

                writer.UpdateGlobalFrameCount();
                Console.WriteLine("Wrote 10 frames of data to " + fiTarget.Name);

            }

     
        }
    }
}