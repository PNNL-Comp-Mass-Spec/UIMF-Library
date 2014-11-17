using System;
using System.IO;
using System.Reflection;

namespace UIMFLibrary_Demo
{
    static class Program
    {
        private const bool TEST_READER = false;

        private static void Main(string[] args)
        {
            string dataFilePathForReader = string.Empty;

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
                runner.Execute();
            }

            WriterTest();

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

                using (var writer = new UIMFLibrary.DataWriter(fiTestFile.FullName))
                {
                    writer.CreateTables();

                    var globalParams = new UIMFLibrary.GlobalParameters
                    {
                        BinWidth = 1,
                        TOFIntensityType = "int",
                        DateStarted = DateTime.Now.ToString()
                    };

                    writer.InsertGlobal(globalParams);

                    globalParams = writer.GetGlobalParameters();
                    writer.UpdateGlobalParameter("TimeOffset", "1");

                    writer.UpdateGlobalParameter("Instrument_Name", "IMS_Test");

                    Console.WriteLine("Adding frame 1");

                    const float SECONDS_PER_FRAME = 1.25f;

                    var randGenerator = new Random();

                    for (int frameNum = 1; frameNum < 10; frameNum++)
                    {
                        var fp = new UIMFLibrary.FrameParameters
                        {
                            FragmentationProfile = new double[2],
                            FrameNum = frameNum,
                            FrameType = UIMFLibrary.DataReader.FrameType.MS1,
                            CalibrationSlope = 0.3476349957054481,
                            CalibrationIntercept = 0.034341488647460935,
                            AverageTOFLength = 163366.6666666667,
                            StartTime = frameNum * SECONDS_PER_FRAME
                        };

                        writer.InsertFrame(fp);

                        for (int scanNumber = 1; scanNumber < 600; scanNumber++)
                        {
                            if (scanNumber % 100 == 0)
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

                            writer.InsertScan(fp, scanNumber, intensities, globalParams.BinWidth);
                        }

                        if (frameNum % 2 == 0)
                            writer.FlushUimf();
                    }

                    writer.FlushUimf();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error testing the writer: " + ex.Message);
            }

        }
    }
}
