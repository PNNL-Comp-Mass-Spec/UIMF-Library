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
    public class CreateUIMFFileTests
    {
        [Test]
        [TestCase(10)]
        public void CreateDB_test1(int frameCountToWrite)
        {
            DataReaderTests.DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            var fiTarget = new FileInfo(FileRefs.WriterTest10Frames);

            if (fiTarget.Exists)
            {
                fiTarget.Delete();
            }

            var executingAssembly = System.Reflection.Assembly.GetExecutingAssembly();
            using (var writer = new DataWriter(fiTarget.FullName, executingAssembly))
            {
                writer.CreateTables("int");

                var globalParameters = new GlobalParams();

                globalParameters.AddUpdateValue(GlobalParamKeyType.Bins, 400000)
                                .AddUpdateValue(GlobalParamKeyType.BinWidth, 0.25)
                                .AddUpdateValue(GlobalParamKeyType.DateStarted, DateTime.Now)
                                .AddUpdateValue(GlobalParamKeyType.NumFrames, frameCountToWrite)
                                .AddUpdateValue(GlobalParamKeyType.TOFIntensityType, "ADC");

                writer.InsertGlobal(globalParameters)
                      .AddUpdateGlobalParameter(GlobalParamKeyType.TimeOffset, 1)
                      .AddUpdateGlobalParameter(GlobalParamKeyType.InstrumentName, "IMS_Test");

                Console.WriteLine("Adding frame 1");

                const float SECONDS_PER_FRAME = 1.25f;

                var randGenerator = new Random();

                for (var frameNum = 1; frameNum <= frameCountToWrite; frameNum++)
                {
                    var fp = new FrameParams();

                    fp.AddUpdateValue(FrameParamKeyType.FrameType, (int)UIMFData.FrameType.MS1)
                      .AddUpdateValue(FrameParamKeyType.CalibrationSlope, 0.3476349957054481)
                      .AddUpdateValue(FrameParamKeyType.CalibrationIntercept, 0.03434148864746093)
                      .AddUpdateValue(FrameParamKeyType.AverageTOFLength, 163366.6666666667)
                      .AddUpdateValue(FrameParamKeyType.StartTimeMinutes, frameNum * SECONDS_PER_FRAME)
                      .AddUpdateValue(FrameParamKeyType.Scans, 600);

                    writer.InsertFrame(frameNum, fp);

                    for (var scanNumber = 1; scanNumber <= 600; scanNumber++)
                    {
                        if (scanNumber == 1 || scanNumber % 100 == 0)
                            Console.WriteLine("Adding frame " + frameNum + ", scan " + scanNumber);

                        var intensities = new int[148000];

                        for (var i = 0; i < intensities.Length; i++)
                        {
                            var nextRandom = randGenerator.Next(0, 255);
                            if (nextRandom < 250)
                                intensities[i] = 0;
                            else
                                intensities[i] = nextRandom;
                        }

                        writer.InsertScan(frameNum, fp, scanNumber, intensities, globalParameters.BinWidth);
                    }
                }

                writer.UpdateGlobalStats();

                Console.WriteLine("Wrote 10 frames of data to " + fiTarget.FullName);
            }
        }
    }
}