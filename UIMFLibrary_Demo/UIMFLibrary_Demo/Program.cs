using System;
using System.IO;

namespace UIMFLibrary_Demo
{
	static class Program
	{
		static void Main(string[] args)
		{

			if (args == null || args.Length == 0)
			{
				Console.WriteLine("Please provide the path to a UIMF file.");
				System.Threading.Thread.Sleep(3000);
				return;
			}

			var runner = new DemoRunner(args[0]);
			runner.Execute();

			WriterTest();

			System.Threading.Thread.Sleep(1000);

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
						TOFIntensityType= "int",
						DateStarted = DateTime.Now.ToString()
					};

					writer.InsertGlobal(globalParams);

					globalParams = writer.GetGlobalParameters();
					writer.UpdateGlobalParameter("TimeOffset", "1");

					writer.UpdateGlobalParameter("Instrument_Name", "IMS_Test");

					Console.WriteLine("Adding frame 1");

					var fp = new UIMFLibrary.FrameParameters
					{
						FragmentationProfile = new double[2],
						FrameNum = 1,
						FrameType = UIMFLibrary.DataReader.FrameType.MS1,
						CalibrationSlope = 0.3476349957054481,
						CalibrationIntercept = 0.034341488647460935,
						AverageTOFLength = 163366.6666666667
					};

					writer.InsertFrame(fp);

					for (int scanNumber = 1; scanNumber < 600; scanNumber++)
					{
						if (scanNumber % 10 == 0)
							Console.WriteLine("Adding frame 1, scan " + scanNumber);


						var randGenerator = new Random();
						var intensities = new int[148000];

						for (int i = 0; i < intensities.Length; i++)
						{
							int nextRandom = randGenerator.Next(0, 255);
							if (nextRandom < 200)
								intensities[i] = 0;
							else
								intensities[i] = nextRandom;

						}

						writer.InsertScan(fp, scanNumber, intensities, globalParams.BinWidth);
					}
					
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine("Error testing the writer: " + ex.Message);
			}

		}
	}
}
