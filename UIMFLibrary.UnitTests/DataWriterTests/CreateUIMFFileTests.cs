using System;
using System.IO;
using System.Reflection;
using System.Threading;
using NUnit.Framework;

namespace UIMFLibrary.UnitTests.DataWriterTests
{
    /// <summary>
    /// Create UIMF file tests
    /// </summary>
    [TestFixture]
    public class CreateUIMFFileTests
    {
        [Test]
        [TestCase(10)]
        public void CreateDB_test1(int frameCountToWrite)
        {
            DataReaderTests.DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

            var targetFile = new FileInfo(FileRefs.WriterTest10Frames);

            var attemptNumber = 0;
            var rand = new Random();
            var nameCustomized = false;

            while (targetFile.Exists && attemptNumber < 10)
            {
                attemptNumber++;

                try
                {
                    Console.WriteLine("Deleting file {0}", targetFile.FullName);
                    targetFile.Delete();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error deleting file {0} on attempt {1}: {2}", targetFile.FullName, attemptNumber, ex.Message);

                    var sleepSeconds = attemptNumber * 10;
                    Console.WriteLine("Sleeping for {0} seconds", sleepSeconds);
                    Thread.Sleep(sleepSeconds * 1000);

                    // Customize the filename by appending a random number
                    targetFile = new FileInfo(Path.Combine(
                        targetFile.DirectoryName ?? string.Empty,
                        string.Format("{0}_{1}{2}",
                            Path.GetFileNameWithoutExtension(targetFile.Name),
                            rand.Next(1000),
                            Path.GetExtension(targetFile.Name))));

                    Console.WriteLine("Updated target file: {0}", targetFile.FullName);
                    nameCustomized = true;
                }

                targetFile.Refresh();
            }

            var executingAssembly = Assembly.GetExecutingAssembly();
            using (var writer = new DataWriter(targetFile.FullName, executingAssembly))
            {
                writer.CreateTables(executingAssembly);

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
                    var frameParams = new FrameParams(frameNum);

                    frameParams.AddUpdateValue(FrameParamKeyType.FrameType, (int)UIMFData.FrameType.MS1)
                      .AddUpdateValue(FrameParamKeyType.CalibrationSlope, 0.3476349957054481)
                      .AddUpdateValue(FrameParamKeyType.CalibrationIntercept, 0.03434148864746093)
                      .AddUpdateValue(FrameParamKeyType.AverageTOFLength, 163366.6666666667)
                      .AddUpdateValue(FrameParamKeyType.StartTimeMinutes, frameNum * SECONDS_PER_FRAME)
                      .AddUpdateValue(FrameParamKeyType.Scans, 600);

                    writer.InsertFrame(frameNum, frameParams);

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

                        writer.InsertScan(frameNum, frameParams, scanNumber, intensities, globalParameters.BinWidth);
                    }
                }

                writer.UpdateGlobalStats();

                Console.WriteLine("Wrote 10 frames of data to " + targetFile.FullName);
            }

            if (!nameCustomized)
            {
                return;
            }

            try
            {
                targetFile.Delete();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error deleting customized target file {0}: {1}", targetFile.FullName, ex.Message);
            }
        }
    }
}