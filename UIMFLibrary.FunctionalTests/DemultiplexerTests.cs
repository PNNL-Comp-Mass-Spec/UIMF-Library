using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using IMSDemultiplexer;
using MathNet.Numerics.LinearAlgebra.Double;
using NUnit.Framework;
using UIMFLibrary.UnitTests.DataReaderTests;

namespace UIMFLibrary.FunctionalTests
{
    /// <summary>
    /// Demultiplexer tests
    /// </summary>
    class DemultiplexerTests
    {
        [Test]
        public void TestDemultiplexUIMFSingleFrame()
        {
            // Old: const int numSegments = 24;
            const int numberOfBits = 4;
            const int lcFramesToSum = 1;

            const string matrixString = "100110101111000";

            var multiplierMatrix = MatrixCreator.CreateMatrixForDemultiplexing(matrixString);
            var scaledMatrix = (DenseMatrix)multiplierMatrix.Multiply(2.0 / 16.0);
            var inversedScaledMatrix = (DenseMatrix)scaledMatrix.Inverse();

            var uimfFile = DataReaderTests.VerifyLocalUimfFile(@"Test_Data\9pep_mix_1uM_4bit_50_12Dec11_encoded.uimf");

            if (uimfFile == null)
            {
                var currentDirectory = new DirectoryInfo(".");
                Assert.Fail("UIMF file not found; Current working directory is " + currentDirectory.FullName);
            }

            Console.WriteLine("Opening " + uimfFile.FullName);

            var uimfReader = new DataReader(uimfFile.FullName);

            var globalParams = uimfReader.GetGlobalParams();
            var binWidth = globalParams.BinWidth;
            var frameNumbers = new List<int>(uimfReader.GetFrameNumbers(UIMFData.FrameType.MS1));
            frameNumbers.AddRange(uimfReader.GetFrameNumbers(UIMFData.FrameType.Calibration));
            frameNumbers.AddRange(uimfReader.GetFrameNumbers(UIMFData.FrameType.MS2));
            frameNumbers.Sort();
            var numFrames = frameNumbers.Count;
            Console.WriteLine("Total Data Frames = " + numFrames);

            var baseFileName = Path.GetFileNameWithoutExtension(uimfFile.Name);

            string newFileName;
            if (baseFileName.Contains("encoded"))
            {
                newFileName = baseFileName.Replace("encoded", "decoded") + ".uimf";
            }
            else
            {
                newFileName = baseFileName + "_decoded.uimf";
            }

            var newFile = new FileInfo(Path.Combine(uimfFile.DirectoryName, newFileName));

            var bitSequence = DemultiplexerOptions.GetBitSequence(numberOfBits);

            if (newFile.Exists)
            {
                Console.WriteLine("Deleting existing decoded UIMF file: " + newFile.FullName);
                try
                {
                    newFile.Delete();
                }
                catch (Exception ex)
                {
                    Assert.Fail("Error deleting file: " + ex.Message);
                }

            }

            Console.WriteLine("Demultiplexing to create " + newFile.FullName);

            var executingAssembly = System.Reflection.Assembly.GetExecutingAssembly();
            using (var uimfWriter = new DataWriter(newFile.FullName, executingAssembly))
            {
                uimfWriter.CreateTables(globalParams.GetValue(GlobalParamKeyType.DatasetType));
                uimfWriter.InsertGlobal(globalParams);


                var firstFrameParams = uimfReader.GetFrameParams(1);

                var averageTOFLength = firstFrameParams.GetValueDouble(FrameParamKeyType.AverageTOFLength);

                // Old: Demultiplexer demultiplexer = new Demultiplexer(inversedScaledMatrix, numSegments, binWidth, averageTOFLength);
                var demultiplexerOptions = new DemultiplexerOptions(uimfFile.FullName, bitSequence);
                var demultiplexer = new Demultiplexer(demultiplexerOptions);
                var scanToIndexMap = demultiplexerOptions.ScanToIndexMap;

                var segmentLength = demultiplexer.NumberOfSegments;

                foreach (var currentFrameNumber in frameNumbers)
                {
                    Console.WriteLine("Processing Frame " + currentFrameNumber);

                    // Setup the frame in the UIMFWriter
                    var frameParams = uimfReader.GetFrameParams(currentFrameNumber);
                    uimfWriter.InsertFrame(currentFrameNumber, frameParams);

                    // If we are dealing with a calibration frame, we just want to write the data to the new UIMF file as is
                    if (frameParams.FrameType == UIMFData.FrameType.Calibration)
                    {
                        for (var i = 0; i < frameParams.Scans; i++)
                        {
                            var intensities = uimfReader.GetSpectrumAsBins(
                                currentFrameNumber,
                                currentFrameNumber,
                                frameParams.FrameType,
                                startScanNumber: i,
                                endScanNumber: i);

                            var nonZeroCount = intensities.Count(x => x > 0);
                            uimfWriter.InsertScan(currentFrameNumber, frameParams, i, intensities, binWidth);
                        }

                        continue;
                    }

                    // Data pulled from the UIMF file will not be re-ordered
                    const bool isReordered = false;

                    // Get the data of the frame from the UIMF File
                    var arrayOfIntensityArrays = uimfReader.GetIntensityBlockForDemultiplexing(
                        currentFrameNumber,
                        frameParams.FrameType,
                        segmentLength,
                        scanToIndexMap,
                        isReordered,
                        lcFramesToSum);

                    // Demultiplex the frame, which updates the array
                    var scanDataEnumerable = demultiplexer.DemultiplexFrame(arrayOfIntensityArrays, isReordered);

                    var sortByScanNumberQuery = (from scanData in scanDataEnumerable
                                                 orderby scanData.ScanNumber
                                                 select scanData).ToList();

                    Assert.AreEqual(93, sortByScanNumberQuery.Count);

                    foreach (var scanData in sortByScanNumberQuery)
                    {
                        var dataPoints = new List<Tuple<int, int>>(scanData.BinsToIntensitiesMap.Count);
                        dataPoints.AddRange(from dataPoint in scanData.BinsToIntensitiesMap
                                            where dataPoint.Value != 0
                                            orderby dataPoint.Key
                                            select new Tuple<int, int>(dataPoint.Key, dataPoint.Value));


                        uimfWriter.InsertScan(
                            currentFrameNumber,
                            frameParams,
                            scanData.ScanNumber,
                            dataPoints,
                            binWidth,
                            0);
                    }

                    uimfWriter.FlushUimf();

                }
            }
        }

    }
}
