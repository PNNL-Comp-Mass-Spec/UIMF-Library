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
        private const bool UPDATE_PARAM_TABLES = false;
        private const bool ADD_LEGACY_PARAM_TABLES = true;

        private static void Main(string[] args)
        {
            string dataFilePathForReader;

            if (args != null && args.Length > 0)
                dataFilePathForReader = args[0];
            else
            {
                // Look for the default file
                var diDataFolder = new DirectoryInfo(GetAppFolderPath());
                DirectoryInfo diDataFolderPath = null;
                while (true)
                {
                    var diFolders = diDataFolder.GetDirectories("Test_Data");
                    if (diFolders.Length > 0)
                    {
                        diDataFolderPath = diFolders[0];
                        break;
                    }

                    if (diDataFolder.Parent == null)
                    {
                        break;
                    }
                    diDataFolder = diDataFolder.Parent;
                }

                if (diDataFolderPath == null)
                {
                    Console.WriteLine("Please provide the path to a UIMF file.");
                    System.Threading.Thread.Sleep(3000);
                    return;
                }

                var fiFiles = diDataFolderPath.GetFiles("*.uimf");
                if (fiFiles.Length == 0)
                {
                    Console.WriteLine("No .UIMF files were found in folder " + diDataFolder.FullName);
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
                const string legacyFilePath = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000_v2.uimf";
                UpdateParamTables(legacyFilePath);
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
                var fiLegacyFile = new FileInfo(uimfFilePath);
                if (!fiLegacyFile.Exists)
                    return;

                if (fiLegacyFile.Directory == null)
                    return;

                var targetPath = Path.Combine(fiLegacyFile.Directory.FullName,
                                              Path.GetFileNameWithoutExtension(fiLegacyFile.Name) + "_updated.uimf");

                Console.WriteLine("Duplicating " + fiLegacyFile.FullName + Environment.NewLine + " to create " +Path.GetFileName(targetPath));

                fiLegacyFile.CopyTo(targetPath, true);
                System.Threading.Thread.Sleep(100);

                // For an exising .UIMF file, simply opening the file with the writer will update the tables
                using (var writer = new DataWriter(targetPath))
                {
                    writer.UpdateGlobalFrameCount();
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
                    globalParameters.AddUpdateValue(GlobalParamKeyType.BinWidth, 1);
                    globalParameters.AddUpdateValue(GlobalParamKeyType.TOFIntensityType, "int");
                    globalParameters.AddUpdateValue(GlobalParamKeyType.DateStarted, DateTime.Now);
                    globalParameters.AddUpdateValue(GlobalParamKeyType.TOFCorrectionTime, 0.0);

                    writer.InsertGlobal(globalParameters);

                    globalParameters = writer.GetGlobalParams();
                    writer.AddUpdateGlobalParameter(GlobalParamKeyType.TimeOffset, 1);
                    writer.AddUpdateGlobalParameter(GlobalParamKeyType.InstrumentName, "IMS_Test");
                    writer.FlushUimf();

                    const float SECONDS_PER_FRAME = 1.25f;

                    var randGenerator = new Random();

                    for (int frameNum = 1; frameNum <= 5; frameNum++)
                    {
                        var fp = new FrameParams();

                        fp.AddUpdateValue(FrameParamKeyType.FrameType, (int)UIMFLibrary.DataReader.FrameType.MS1);
                        fp.AddUpdateValue(FrameParamKeyType.CalibrationSlope, 0.3476349957054481);
                        fp.AddUpdateValue(FrameParamKeyType.CalibrationIntercept, 0.03434148864746093);
                        fp.AddUpdateValue(FrameParamKeyType.AverageTOFLength, 163366.6666666667);
                        fp.AddUpdateValue(FrameParamKeyType.StartTimeMinutes, frameNum * SECONDS_PER_FRAME);

                        writer.InsertFrame(frameNum, fp);

                        writer.AddUpdateFrameParameter(frameNum, FrameParamKeyType.Accumulations, "18");
                        writer.AddUpdateFrameParameter(frameNum, FrameParamKeyType.TOFLosses, randGenerator.Next(0, 150).ToString());

                        for (int scanNumber = 1; scanNumber < 600; scanNumber++)
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
