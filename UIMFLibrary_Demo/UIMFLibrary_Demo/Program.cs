using System;
using System.IO;
using System.Reflection;
using UIMFLibrary;

namespace UIMFLibrary_Demo
{
    static class Program
    {
        private const bool TEST_READER = false;

        private const bool TEST_WRITER = false;

        /// <summary>
        /// Set this to True to add the Frame_Params and Global_Params tables
        /// </summary>
        private const bool UPDATE_PARAM_TABLES = true;

        /// <summary>
        /// When true, duplicate the .uimf file to add the missing tables
        /// When false, simply add the tables
        /// </summary>
        /// <remarks>Only used when UPDATE_PARAM_TABLES is true</remarks>
        private const bool UPDATE_PARAM_TABLES_DUPLICATE_FILE = false;

        private const bool ADD_LEGACY_PARAM_TABLES = false;

        private static void Main(string[] args)
        {
            string dataFilePathForReader;

            if (args != null && args.Length > 0)
            {
                dataFilePathForReader = args[0];
            }
            else
            {
                Console.WriteLine("Looking for a directory named Test_Data");

                // Find the first directory with a .uimf file
                var testDataDirectory = FindTestDataDirectory();

                var inputDirectory = testDataDirectory ?? FindDirectoryWithUimfFile();

                if (inputDirectory == null)
                {
                    Console.WriteLine("Please provide the path to a UIMF file, " +
                                      "or run this program in a directory with a UIMF file " +
                                      "or in a directory with subdirectory Test_Data");
                    System.Threading.Thread.Sleep(3000);
                    return;
                }

                var fiFiles = inputDirectory.GetFiles("*.uimf");
                if (fiFiles.Length == 0)
                {
                    Console.WriteLine("No .UIMF files were found in directory " + inputDirectory.FullName);
                    System.Threading.Thread.Sleep(3000);
                    return;
                }

                dataFilePathForReader = fiFiles[0].FullName;

            }

            if (TEST_READER)
            {
                var runner = new DemoRunner(dataFilePathForReader);

                runner.ReadAllFramesAndScans();
                runner.Execute();
            }

            if (UPDATE_PARAM_TABLES)
            {
                UpdateParamTables(dataFilePathForReader);
            }

            if (TEST_WRITER)
            {
                WriterTest();
            }

            if (ADD_LEGACY_PARAM_TABLES)
            {
                const string v3FilePath = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000_v3.uimf";

                AddLegacyParamTables(v3FilePath);

            }

            System.Threading.Thread.Sleep(1000);

        }

        /// <summary>
        /// Search for a .uimf file in the current directory and all subdirectories
        /// If not files are found, move up one level and try again
        /// </summary>
        /// <returns></returns>
        private static DirectoryInfo FindDirectoryWithUimfFile()
        {
            // Look for the first directory with a UIMF file
            var directoryToCheck = new DirectoryInfo(GetAppFolderPath());

            while (true)
            {
                var uimfFiles = directoryToCheck.GetFiles("*.uimf");
                if (uimfFiles.Length > 0)
                {
                    return directoryToCheck;
                }

                var subdirectories = directoryToCheck.GetDirectories();
                foreach (var subDirectory in subdirectories)
                {
                    var subDirUimfFiles = subDirectory.GetFiles("*.uimf");
                    if (subDirUimfFiles.Length > 0)
                    {
                        return subDirectory;
                    }
                }

                if (directoryToCheck.Parent == null)
                {
                    return null;
                }

                directoryToCheck = directoryToCheck.Parent;
            }
        }

        private static DirectoryInfo FindTestDataDirectory()
        {
            // Look for the default file
            var directoryToCheck = new DirectoryInfo(GetAppFolderPath());

            while (true)
            {
                var subDirectories = directoryToCheck.GetDirectories("Test_Data");
                if (subDirectories.Length > 0)
                {
                    return subDirectories[0];
                }

                if (directoryToCheck.Parent == null)
                {
                    return null;
                }
                directoryToCheck = directoryToCheck.Parent;
            }

        }

        private static string GetAppFolderPath()
        {
            return Path.GetDirectoryName(GetAppPath());
        }

        private static string GetAppPath()
        {
            return Assembly.GetExecutingAssembly().Location;
        }

        private static void AddLegacyParamTables(string uimfFilePath)
        {
            try
            {
                var fiUimfFile = new FileInfo(uimfFilePath);
                if (!fiUimfFile.Exists)
                    return;

                if (fiUimfFile.Directory == null)
                    return;

                var targetPath = Path.Combine(fiUimfFile.Directory.FullName,
                                              Path.GetFileNameWithoutExtension(fiUimfFile.Name) + "_LegacyTablesAdded.uimf");

                var fiTargetFile = new FileInfo(targetPath);
                Console.WriteLine("Duplicating " + fiUimfFile.FullName + Environment.NewLine + " to create " + fiTargetFile.FullName);

                fiUimfFile.CopyTo(fiTargetFile.FullName, true);
                fiTargetFile.Refresh();

                fiTargetFile.LastWriteTimeUtc = DateTime.UtcNow;

                var journalFilePath = targetPath + "-journal";
                if (File.Exists(journalFilePath))
                    File.Delete(journalFilePath);

                System.Threading.Thread.Sleep(100);

                using (var writer = new DataWriter(targetPath))
                {
                    writer.AddLegacyParameterTablesUsingExistingParamTables();
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in AddLegacyParamTables: " + ex.Message);
            }
        }

        private static void UpdateParamTables(string uimfFilePath)
        {
            try
            {
                var legacyUimfFile = new FileInfo(uimfFilePath);
                if (!legacyUimfFile.Exists)
                {
                    ConsoleMsgUtils.ShowWarning("File not found: " + uimfFilePath);
                    return;
                }


                if (UPDATE_PARAM_TABLES_DUPLICATE_FILE)
                {

                    if (legacyUimfFile.Directory == null)
                    {
                        ConsoleMsgUtils.ShowWarning("Cannot determine the parent directory of: " + uimfFilePath);
                        return;
                    }

                    var targetPath = Path.Combine(legacyUimfFile.Directory.FullName,
                                                  Path.GetFileNameWithoutExtension(legacyUimfFile.Name) + "_updated.uimf");

                    Console.WriteLine("Duplicating " + legacyUimfFile.FullName + Environment.NewLine + " to create " + Path.GetFileName(targetPath));

                    legacyUimfFile.CopyTo(targetPath, true);
                    System.Threading.Thread.Sleep(100);

                    // For an existing .UIMF file, simply opening the file with the writer will update the tables
                    using (var writer = new DataWriter(targetPath))
                    {
                        writer.UpdateGlobalStats();
                    }
                }
                else
                {
                    Console.WriteLine("Updating " + legacyUimfFile.FullName + Environment.NewLine + " to add the Global_Params and Frame_Params tables");

                    using (var writer = new DataWriter(legacyUimfFile.FullName))
                    {
                        Console.WriteLine("  .. updating global stats");
                        writer.UpdateGlobalStats();
                        Console.WriteLine("  .. update complete");
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in UpdateParamTables: " + ex.Message);
            }
        }

        private static void WriterTest()
        {
            var fiTestFile = new FileInfo("TestOutput.uimf");

            try
            {
                if (fiTestFile.Exists)
                    fiTestFile.Delete();
            }
            catch (IOException)
            {
                Console.WriteLine("Existing " + fiTestFile.Name + " file exists and cannot be deleted (locked)");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Existing " + fiTestFile.Name + " file exists and cannot be deleted: " + ex.Message);
                return;
            }

            try
            {
                Console.WriteLine("Creating " + fiTestFile.FullName);

                using (var writer = new DataWriter(fiTestFile.FullName))
                {
                    writer.CreateTables();

                    var globalParameters = new GlobalParams();
                    globalParameters.AddUpdateValue(GlobalParamKeyType.Bins, 400000)
                                    .AddUpdateValue(GlobalParamKeyType.BinWidth, 0.25)
                                    .AddUpdateValue(GlobalParamKeyType.DateStarted, DateTime.Now)
                                    .AddUpdateValue(GlobalParamKeyType.TOFIntensityType, "ADC")                                    
                                    .AddUpdateValue(GlobalParamKeyType.TOFCorrectionTime, 0.0);

                    writer.InsertGlobal(globalParameters);

                    globalParameters = writer.GetGlobalParams();

                    writer.AddUpdateGlobalParameter(GlobalParamKeyType.TimeOffset, 1)
                          .AddUpdateGlobalParameter(GlobalParamKeyType.InstrumentName, "IMS_Test")
                          .FlushUimf();

                    const float SECONDS_PER_FRAME = 1.25f;

                    var randGenerator = new Random();

                    for (var frameNum = 1; frameNum <= 5; frameNum++)
                    {
                        var fp = new FrameParams();

                        fp.AddUpdateValue(FrameParamKeyType.FrameType, (int)UIMFLibrary.DataReader.FrameType.MS1)
                          .AddUpdateValue(FrameParamKeyType.CalibrationSlope, 0.3476349957054481)
                          .AddUpdateValue(FrameParamKeyType.CalibrationIntercept, 0.03434148864746093)
                          .AddUpdateValue(FrameParamKeyType.AverageTOFLength, 163366.6666666667)
                          .AddUpdateValue(FrameParamKeyType.StartTimeMinutes, frameNum * SECONDS_PER_FRAME);

                        writer.InsertFrame(frameNum, fp)
                              .AddUpdateFrameParameter(frameNum, FrameParamKeyType.Accumulations, "18")
                              .AddUpdateFrameParameter(frameNum, FrameParamKeyType.TOFLosses, randGenerator.Next(0, 150).ToString());

                        for (var scanNumber = 1; scanNumber < 600; scanNumber++)
                        {
                            if (scanNumber == 1 | scanNumber % 100 == 0)
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
                }

                Console.WriteLine("Wrote 5 frames of data to \n" + fiTestFile.FullName);
                
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error testing the writer: " + ex.Message);
            }

        }
    }
}
